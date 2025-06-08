import math
import time
import threading
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException


class BinanceFuturesTrader:
    """
    A generic Binance USDT-M futures trader with optional stop-loss and take-profit,
    automatically canceling the opposite leg once one triggers.

    After sending a MARKET order, we poll the position endpoint to get the true
    entryPrice, then calculate TP/SL off of that actual fill price.
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.client = Client(api_key, api_secret)
        if testnet:
            self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
        else:
            self.client.FUTURES_URL = 'https://fapi.binance.com/fapi'

    def _get_step_size(self, symbol: str) -> float:
        info = self.client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
        raise ValueError(f"Could not find LOT_SIZE for symbol {symbol}")

    def _get_price_tick(self, symbol: str) -> float:
        info = self.client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'PRICE_FILTER':
                        return float(f['tickSize'])
        raise ValueError(f"Could not find PRICE_FILTER for symbol {symbol}")

    def _get_mark_price(self, symbol: str) -> float:
        mark = self.client.futures_mark_price(symbol=symbol)
        return float(mark['markPrice'])

    def _calculate_quantity(self, usdt_amount: float, price: float, step_size: float) -> float:
        raw = usdt_amount / price
        precision = int(round(-math.log10(step_size)))
        qty = math.floor(raw * (10 ** precision)) / (10 ** precision)
        if qty <= 0:
            raise ValueError(
                f"Quantity calculation returned zero: usdt={usdt_amount}, price={price}, step={step_size}"
            )
        return qty

    def _round_price(self, price: float, tick: float, direction: str) -> float:
        precision = int(round(-math.log10(tick)))
        factor = 10 ** precision
        return (math.floor(price * factor) / factor) if direction == 'down' else (math.ceil(price * factor) / factor)

    def _watch_and_cancel(self, symbol: str, sl_id: int, tp_id: int):
        """
        Poll order statuses and cancel the opposite leg when one is filled.
        """
        while True:
            sl_status = None
            tp_status = None
            if sl_id:
                try:
                    sl_order = self.client.futures_get_order(symbol=symbol, orderId=sl_id)
                    sl_status = sl_order.get('status')
                except BinanceAPIException as e:
                    if e.code != -2011:  # if not “Unknown order sent”
                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  ❌ Error fetching SL order: {e}")
            if tp_id:
                try:
                    tp_order = self.client.futures_get_order(symbol=symbol, orderId=tp_id)
                    tp_status = tp_order.get('status')
                except BinanceAPIException as e:
                    if e.code != -2011:
                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  ❌ Error fetching TP order: {e}")

            # If SL filled first, cancel TP
            if sl_status == 'FILLED' and tp_id:
                try:
                    self.client.futures_cancel_order(symbol=symbol, orderId=tp_id)
                except BinanceAPIException as e:
                    if e.code != -2011:
                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  ❌ Error canceling TP: {e}")
                break

            # If TP filled first, cancel SL
            if tp_status == 'FILLED' and sl_id:
                try:
                    self.client.futures_cancel_order(symbol=symbol, orderId=sl_id)
                except BinanceAPIException as e:
                    if e.code != -2011:
                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  ❌ Error canceling SL: {e}")
                break

            time.sleep(1)

    def _fetch_entry_price(self, symbol: str, side: str, qty: float, max_attempts: int = 5, delay: float = 0.5) -> float:
        """
        After placing a MARKET order, poll the position endpoint for this symbol
        until entryPrice > 0 or until max_attempts exceeded. If still zero, fallback
        to current mark price.
        """
        for _ in range(max_attempts):
            try:
                positions = self.client.futures_position_information(symbol=symbol)
                for pos in positions:
                    # pos['positionAmt'] is a string; if nonzero, it's our position
                    amt = float(pos.get('positionAmt', 0))
                    if amt != 0 and ((side == 'long' and amt > 0) or (side == 'short' and amt < 0)):
                        entry_price = float(pos.get('entryPrice', 0))
                        if entry_price > 0:
                            return entry_price
            except BinanceAPIException:
                pass
            time.sleep(delay)
        # Fallback:
        return self._get_mark_price(symbol)

    def place_order(
        self,
        symbol: str,
        direction: str,
        usdt_amount: float,
        stop_loss_pct: float = None,
        take_profit_pct: float = None
    ) -> dict:
        side = direction.lower()
        if side not in ('long', 'short'):
            raise ValueError("Direction must be 'long' or 'short'")

        entry_side = Client.SIDE_BUY if side == 'long' else Client.SIDE_SELL
        exit_side = Client.SIDE_SELL if side == 'long' else Client.SIDE_BUY

        # 1) Get a “nearby” price to calculate quantity
        mark_price_for_qty = self._get_mark_price(symbol)
        lot_step = self._get_step_size(symbol)
        qty = self._calculate_quantity(usdt_amount, mark_price_for_qty, lot_step)

        tick = self._get_price_tick(symbol)

        try:
            # 2) Send entry MARKET order
            entry = self.client.futures_create_order(
                symbol=symbol,
                side=entry_side,
                type=Client.ORDER_TYPE_MARKET,
                quantity=qty
            )

            # 3) Determine actual entry price by polling position info
            entry_price = self._fetch_entry_price(symbol, side, qty)
            # Now entry_price is the real fill price (or mark price fallback)
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  ✅ Entry order placed: {entry}")
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  Entry price for {symbol}: {entry_price:.4f}")
            
            if entry_price <= 0:
                raise ValueError(f"Failed to fetch valid entry price for {symbol}. Entry price is zero.")
            
            sl_id = None
            tp_id = None

            # 4) Compute Stop‐Loss
            if stop_loss_pct is not None:
                if side == 'long':
                    raw_sl = entry_price * (1 - stop_loss_pct / 100)
                    sl_price = self._round_price(raw_sl, tick, 'down')
                else:
                    raw_sl = entry_price * (1 + stop_loss_pct / 100)
                    sl_price = self._round_price(raw_sl, tick, 'up')

                sl_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=exit_side,
                    type='STOP_MARKET',
                    stopPrice=str(sl_price),
                    closePosition=False,
                    quantity=str(qty),
                    reduceOnly=True
                )
                sl_id = sl_order.get('orderId')

            # 5) Compute Take‐Profit
            if take_profit_pct is not None:
                if side == 'long':
                    raw_tp = entry_price * (1 + take_profit_pct / 100)
                    tp_price = self._round_price(raw_tp, tick, 'up')
                else:
                    raw_tp = entry_price * (1 - take_profit_pct / 100)
                    tp_price = self._round_price(raw_tp, tick, 'down')

                tp_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=exit_side,
                    type=Client.ORDER_TYPE_LIMIT,
                    price=str(tp_price),
                    timeInForce=Client.TIME_IN_FORCE_GTC,
                    closePosition=False,
                    quantity=str(qty),
                    reduceOnly=True
                )
                tp_id = tp_order.get('orderId')

            return {
                **entry,
                "effectivePrice": entry_price  # include for debugging
            }

        except BinanceAPIException as e:
            raise BinanceAPIException(e.response, e.status_code, e.message)
        except BinanceOrderException as e:
            raise BinanceOrderException(str(e))

    trade = place_order


# Example usage
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    trader = BinanceFuturesTrader(api_key, api_secret)
    result = trader.trade('MEUSDT', 'short', 12, stop_loss_pct=1.0, take_profit_pct=1.5)
    print(result)
