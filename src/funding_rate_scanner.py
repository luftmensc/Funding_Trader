# funding_rate_scanner.py
import time
import requests
from datetime import datetime, timezone, timedelta


class FundingRateScanner:
    """
    Scans Binance USDT-M perpetual futures for funding rates,
    filters by a threshold, and finds those with upcoming or recent funding events.
    """

    EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    PREMIUM_INDEX_URL  = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    FUNDING_RATE_URL   = 'https://fapi.binance.com/fapi/v1/fundingRate'
    _CACHE_TTL = 30 * 60  # 30 minutes

    def __init__(self, threshold_pct: float = 0.15):
        if threshold_pct < 0:
            raise ValueError("threshold_pct must be non-negative")
        self.threshold = threshold_pct / 100.0
        self._session = requests.Session()
        self._cached_symbols = None
        self._cached_intervals = None
        self._last_info_ts = 0

    def _request_with_retries(self, url: str, params: dict = None, max_retries: int = 3, timeout: float = 5.0):
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = attempt * 2
                print(f"{datetime.now().isoformat()}  ⚠️  Request to {url} failed (attempt {attempt}/{max_retries}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except requests.exceptions.HTTPError as e:
                print(f"{datetime.now().isoformat()}  ❌  HTTP error from {url}: {e}. Aborting this request.")
                raise
        raise requests.exceptions.ConnectionError(f"Failed to GET {url} after {max_retries} attempts.")

    def _refresh_exchange_info_if_needed(self):
        now_ts = time.time()
        if self._cached_symbols and (now_ts - self._last_info_ts) < FundingRateScanner._CACHE_TTL:
            return
        data = self._request_with_retries(FundingRateScanner.EXCHANGE_INFO_URL)
        self._cached_symbols = {s['symbol'] for s in data.get('symbols', []) if s.get('contractType')=='PERPETUAL'}
        arr = self._request_with_retries(FundingRateScanner.FUNDING_RATE_URL, params={'limit': 1000})
        self._cached_intervals = {e['symbol']: e.get('fundingInterval', 8) for e in arr}
        self._last_info_ts = now_ts

    def _get_perpetual_symbols(self) -> set:
        self._refresh_exchange_info_if_needed()
        return self._cached_symbols or set()

    def _get_intervals(self) -> dict:
        self._refresh_exchange_info_if_needed()
        return self._cached_intervals or {}

    def _get_premium_index(self) -> list:
        return self._request_with_retries(FundingRateScanner.PREMIUM_INDEX_URL)

    def scan(self) -> list:
        symbols = self._get_perpetual_symbols()
        intervals = self._get_intervals()
        try:
            premium = self._get_premium_index()
        except Exception as e:
            print(f"{datetime.now().isoformat()}  ❌  Failed to fetch premiumIndex: {e}")
            return []

        results = []
        now_utc = datetime.now(timezone.utc)
        for entry in premium:
            sym = entry.get('symbol')
            if not sym or sym not in symbols:
                continue
            raw_rate = entry.get('lastFundingRate') or entry.get('fundingRate', 0)
            try:
                rate = float(raw_rate)
            except (ValueError, TypeError):
                continue
            if abs(rate) < self.threshold:
                continue
            ts_ms = entry.get('nextFundingTime')
            if ts_ms is None:
                continue
            next_ts = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
            interval = intervals.get(sym, 8)
            results.append({
                'symbol': sym,
                'rate_pct': rate * 100,
                'next_funding': next_ts,
                'interval_h': interval
            })
        # Print results for debugging
        for r in results:
            print(f"{r['symbol']}: {r['rate_pct']:.4f}%, next funding at {r['next_funding'].isoformat()} UTC, interval {r['interval_h']}h")
        # Sort by next funding time
        
        return results

    def get_upcoming_pairs(self, window_sec: int) -> list:
        now = datetime.now(timezone.utc)
        upcoming = []
        for r in self.scan():
            delta = (r['next_funding'] - now).total_seconds()
            if 0 <= delta <= window_sec:
                upcoming.append((r['symbol'], r['rate_pct']))
        return upcoming

    def get_recent_pairs(self, window_sec: int) -> list:
        now = datetime.now(timezone.utc)
        recent = []
        for r in self.scan():
            last_ts = r['next_funding'] - timedelta(hours=r['interval_h'])
            delta = (now - last_ts).total_seconds()
            if 0 <= delta <= window_sec:
                recent.append((r['symbol'], r['rate_pct']))
        return recent


if __name__ == '__main__':
    scanner = FundingRateScanner(threshold_pct=0.1)
    print("Upcoming pairs:", scanner.get_upcoming_pairs(window_sec=300))
