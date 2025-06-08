import os
import time
import logging
from datetime import datetime

from dotenv import load_dotenv
from trade_binance import BinanceFuturesTrader
from funding_rate_scanner import FundingRateScanner
from telegram_alert import TelegramBot
import requests  # needed for safe_get_threshold_pairs

# ─── GLOBAL SETTINGS ──────────────────────────────────────────────────────────
THRESHOLD_PCT            = 0.3    # funding‐rate threshold (in %)
USDT_AMOUNT              = 100.0   # USDT to allocate per trade (per symbol)
WINDOW_SEC               = 10      # seconds before the hour to trigger (xx:59:50 – xx:60:00)

TAKE_PROFIT_BUFFER_PCT   = 0.5     # extra % on top of |funding_rate| for TP
STOP_LOSS_PCT            = 0.5     # % for stop‐loss on the adverse side

LOG_FILE                 = "../log/trading_logs.log"
# ──────────────────────────────────────────────────────────────────────────────


def setup_logger():
    """
    Configure a logger that writes to LOG_FILE and flushes on every record.
    """
    logger = logging.getLogger("FundingHunter")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)s: %(message)s")
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    chfmt = logging.Formatter("%(asctime)s  %(levelname)s: %(message)s")
    ch.setFormatter(chfmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def safe_scan(scanner: FundingRateScanner, max_retries: int = 3, logger=None) -> list:
    """
    Attempts to call scanner.scan(). Retries on ConnectionError or Timeout.
    Returns an empty list if all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return scanner.scan()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = attempt * 2
            msg = f"Scanner.scan() error (attempt {attempt}/{max_retries}): {e}. Retrying in {wait}s..."
            if logger:
                logger.warning(msg)
            else:
                print(f"{datetime.now().isoformat()}  ⚠️  {msg}")
            time.sleep(wait)
        except Exception as e:
            msg = f"Unexpected error in scanner.scan(): {e}"
            if logger:
                logger.error(msg)
            else:
                print(f"{datetime.now().isoformat()}  ❌  {msg}")
            return []
    msg = f"Failed to scan funding rates after {max_retries} attempts."
    if logger:
        logger.error(msg)
    else:
        print(f"{datetime.now().isoformat()}  ❌  {msg}")
    return []


def main():
    # Load API credentials
    load_dotenv()
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    if not api_key or not api_secret:
        raise RuntimeError('Please set BINANCE_API_KEY and BINANCE_API_SECRET in .env')

    # Telegram credentials
    telegram_token = os.getenv('TELEGRAM_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not telegram_token or not telegram_chat_id:
        raise RuntimeError('Please set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env')

    # Initialize Telegram Bot
    bot = TelegramBot(token=telegram_token, chat_id=int(telegram_chat_id))

    # Set up our file‐based logger
    logger = setup_logger()
    logger.info("Starting funding‐rate watcher.")

    # Initialize trader and scanner
    try:
        trader = BinanceFuturesTrader(api_key, api_secret, testnet=False)
    except Exception as e:
        logger.error(f"Failed to initialize BinanceFuturesTrader: {e}")
        return

    scanner = FundingRateScanner(threshold_pct=THRESHOLD_PCT)

    print((
        f"Starting funding‐rate watcher (threshold = {THRESHOLD_PCT:.2f}%).\n"
        f"At each xx:59:50, it will fetch all funding rates, filter by threshold, then trade."
    ))
    logger.info(
        f"Configuration → THRESHOLD_PCT={THRESHOLD_PCT:.2f}%, USDT_AMOUNT={USDT_AMOUNT:.2f}, "
        f"TAKE_PROFIT_BUFFER_PCT={TAKE_PROFIT_BUFFER_PCT:.2f}, STOP_LOSS_PCT={STOP_LOSS_PCT:.2f}"
    )

    while True:
        try:
            now = datetime.now()
            secs_since_hour = now.minute * 60 + now.second
            secs_until_hour = 3600 - secs_since_hour

            if secs_until_hour <= WINDOW_SEC:
                trigger_msg = f"Trigger window ({WINDOW_SEC}s to hour end) detected."
                print(f"{now.isoformat()}  ▶ {trigger_msg}")
                logger.info(trigger_msg)

                # 1) Fetch ALL funding rates (with scan), which already filters by THRESHOLD_PCT
                all_filtered = safe_scan(scanner, logger=logger)
                total_found = len(all_filtered)
                logger.info(f"After threshold filter ({THRESHOLD_PCT:.2f}%), coins left: {total_found}")
                print(f"{now.isoformat()}  ▶ After threshold filter, coins left: {total_found}")

                # 2) For each coin, open a position
                for entry in all_filtered:
                    sym = entry['symbol']
                    rate_pct = entry['rate_pct']  # e.g. 0.42 (%)
                    direction = 'long' if rate_pct > 0 else 'short'

                    try:
                        entry_price = trader._get_mark_price(sym)
                        tick = trader._get_price_tick(sym)
                    except Exception as e:
                        err = f"Failed to fetch price/tick for {sym}: {e}"
                        print(f"{datetime.now().isoformat()}  ❌ {err}")
                        logger.error(err)
                        continue

                    tp_pct = abs(rate_pct) + TAKE_PROFIT_BUFFER_PCT
                    sl_pct = STOP_LOSS_PCT

                    if direction == 'long':
                        raw_sl = entry_price * (1 - sl_pct/100)
                        raw_tp = entry_price * (1 + tp_pct/100)
                        sl_price = trader._round_price(raw_sl, tick, 'down')
                        tp_price = trader._round_price(raw_tp, tick, 'up')
                    else:
                        raw_sl = entry_price * (1 + sl_pct/100)
                        raw_tp = entry_price * (1 - tp_pct/100)
                        sl_price = trader._round_price(raw_sl, tick, 'up')
                        tp_price = trader._round_price(raw_tp, tick, 'down')

                    details = (
                        f"Opening {direction.upper()} on {sym} @ {entry_price:.7f}, "
                        f"TP @ {tp_price:.7f} ({tp_pct:.7f}%), "
                        f"SL @ {sl_price:.7f} ({sl_pct:.7f}%)"
                    )
                    print(f"{datetime.now().isoformat()}  → {details}")
                    logger.info(details)

                    # 2c) Place order
                    try:
                        order = trader.trade(
                            sym,
                            direction,
                            USDT_AMOUNT,
                            stop_loss_pct=sl_pct,
                            take_profit_pct=tp_pct
                        )
                        placed_msg = (
                            f"Order placed: symbol={sym}, direction={direction}, "
                            f"orderId={order.get('orderId')}"
                        )
                        print(f"{datetime.now().isoformat()}    ✅ {placed_msg}")
                        logger.info(placed_msg)

                        # Send Telegram notification
                        msg_text = (
                            f"📈 Position Opened: {direction.upper()} {sym}\n"
                            f"Entry: {entry_price:.7f}\n"
                            f"TP: {tp_price:.7f} ({tp_pct:.7f}%)\n"
                            f"SL: {sl_price:.7f} ({sl_pct:.7f}%)\n"
                            f"Funding Rate: {rate_pct:.7f}%\n"
                            f"Order ID: {order.get('orderId')}"
                        )
                        bot.send_message(msg_text)

                    except Exception as e:
                        err_msg = f"Failed to place {direction} on {sym}: {e}"
                        print(f"{datetime.now().isoformat()}  ❌ {err_msg}")
                        logger.error(err_msg)

                # Sleep past the hour boundary
                time.sleep(secs_until_hour + 1)

            else:
                time.sleep(1)
                print(f"{now.isoformat()}  ⏳ Waiting for trigger window... "
                      f"({secs_until_hour} seconds until next trigger)")

        except KeyboardInterrupt:
            shutdown_msg = "KeyboardInterrupt received. Exiting cleanly."
            print(f"{datetime.now().isoformat()}  🛑 {shutdown_msg}")
            logger.info(shutdown_msg)
            for handler in logger.handlers:
                handler.close()
            break

        except Exception as main_e:
            err_msg = f"UNEXPECTED ERROR IN MAIN LOOP: {main_e}"
            print(f"{datetime.now().isoformat()}  ❌ {err_msg}")
            logger.error(err_msg)
            time.sleep(5)
            continue


if __name__ == '__main__':
    main()