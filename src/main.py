import os
import time
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from trade_binance import BinanceFuturesTrader
from funding_rate_scanner import FundingRateScanner
from telegram_alert import TelegramBot
import requests  # for catching ConnectionError/Timeout

# ─── GLOBAL SETTINGS ──────────────────────────────────────────────────────────
THRESHOLD_PCT = 0.10   # funding‐rate threshold (in %)
USDT_AMOUNT   = 100.0  # USDT to allocate per trade (per symbol)
WINDOW_SEC    = 10    # seconds before hour boundary to trigger
MULTIPLIER    = 2.0   # TP = MULTIPLIER × |rate|
LOG_FILE      = "./log/trading_logs.log"
# ──────────────────────────────────────────────────────────────────────────────


def setup_logger():
    logger = logging.getLogger("FundingHunter")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s: %(message)s"))

    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def safe_upcoming(scanner: FundingRateScanner, window_sec: int, max_retries: int = 3, logger=None):
    """
    Attempts to call scanner.get_upcoming_pairs(window_sec).
    Retries on ConnectionError or Timeout.
    Returns [] if all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return scanner.get_upcoming_pairs(window_sec)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = attempt * 2
            msg = (f"get_upcoming_pairs() error (attempt {attempt}/{max_retries}): {e}. "
                   f"Retrying in {wait}s…")
            if logger:
                logger.warning(msg)
            else:
                print(f"{datetime.now().isoformat()}  ⚠️  {msg}")
            time.sleep(wait)
        except Exception as e:
            msg = f"Unexpected error in get_upcoming_pairs(): {e}"
            if logger:
                logger.error(msg)
            else:
                print(f"{datetime.now().isoformat()}  ❌  {msg}")
            return []
    msg = f"Failed to fetch upcoming pairs after {max_retries} attempts."
    if logger:
        logger.error(msg)
    else:
        print(f"{datetime.now().isoformat()}  ❌  {msg}")
    return []


def main():
    load_dotenv()
    api_key, api_secret = os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET')
    if not api_key or not api_secret:
        raise RuntimeError('Please set BINANCE_API_KEY and BINANCE_API_SECRET in .env')

    telegram_token = os.getenv('TELEGRAM_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not telegram_token or not telegram_chat_id:
        raise RuntimeError('Please set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env')

    bot = TelegramBot(token=telegram_token, chat_id=int(telegram_chat_id))
    logger = setup_logger()
    logger.info("Starting funding‐rate watcher.")

    try:
        trader = BinanceFuturesTrader(api_key, api_secret, testnet=False)
    except Exception as e:
        logger.error(f"Failed to initialize BinanceFuturesTrader: {e}")
        return

    scanner = FundingRateScanner(threshold_pct=THRESHOLD_PCT)

    print(
        f"Starting watcher (threshold = {THRESHOLD_PCT:.2f}%).\n"
        f"Triggering only symbols whose funding falls in next {WINDOW_SEC}s (UTC)."
    )
    logger.info(
        f"Config → THRESHOLD_PCT={THRESHOLD_PCT:.2f}%, USDT_AMOUNT={USDT_AMOUNT:.2f}, "
        f"WINDOW_SEC={WINDOW_SEC}s, MULTIPLIER={MULTIPLIER:.2f}"
    )

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            secs_since_hour = now_utc.minute * 60 + now_utc.second
            secs_until_hour = 3600 - secs_since_hour

            if 1:
                logger.info(f"UTC trigger window ({WINDOW_SEC}s before hour end).")
                print(f"{now_utc.isoformat()}  ▶ Trigger window…")

                upcoming = safe_upcoming(scanner, WINDOW_SEC, logger=logger)
                total = len(upcoming)
                logger.info(f"{total} symbol(s) with funding in next {WINDOW_SEC}s")
                print(f"{now_utc.isoformat()}  ▶ Upcoming candidates: {total}")

                for sym, rate_pct in upcoming:
                    direction = 'long' if rate_pct > 0 else 'short'
                    try:
                        entry_price = trader._get_mark_price(sym)
                        tick = trader._get_price_tick(sym)
                    except Exception as e:
                        err = f"Failed to fetch price/tick for {sym}: {e}"
                        logger.error(err); print(f"{datetime.now().isoformat()}  ❌  {err}")
                        continue

                    tp_pct, sl_pct = abs(rate_pct) * MULTIPLIER, abs(rate_pct)
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
                        f"Opening {direction.upper()} {sym} @ {entry_price:.7f}, "
                        f"TP @ {tp_price:.7f} ({tp_pct:.7f}%), "
                        f"SL @ {sl_price:.7f} ({sl_pct:.7f}%)"
                    )
                    logger.info(details); print(f"{datetime.now().isoformat()}  → {details}")

                    try:
                        order = trader.trade(
                            sym,
                            direction,
                            USDT_AMOUNT,
                            stop_loss_pct=sl_pct,
                            take_profit_pct=tp_pct
                        )
                        logger.info(f"Order placed: {order.get('orderId')} on {sym}")
                        bot.send_message(
                            f"📈 {direction.upper()} {sym}\n"
                            f"Entry: {entry_price:.7f}\n"
                            f"TP: {tp_price:.7f} ({tp_pct:.7f}%)\n"
                            f"SL: {sl_price:.7f} ({sl_pct:.7f}%)\n"
                            f"Rate: {rate_pct:.7f}%\n"
                            f"Order ID: {order.get('orderId')}"
                        )
                    except Exception as e:
                        err = f"Failed to place {direction} on {sym}: {e}"
                        logger.error(err); print(f"{datetime.now().isoformat()}  ❌  {err}")

                # sleep until just past the hour boundary, then resume waiting
                time.sleep(secs_until_hour + 1)

            else:
                # not in trigger window yet
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received; exiting cleanly.")
            break

        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)
            continue


if __name__ == '__main__':
    main()