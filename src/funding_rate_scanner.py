import time
import requests
from datetime import datetime, timezone, timedelta


class FundingRateScanner:
    """
    Scans Binance USDT-M perpetual futures for funding rates,
    filters by a threshold, and finds those with upcoming or recent funding events.

    This version uses a requests.Session, timeouts, caching of exchangeInfo,
    and small retry loops to avoid single‐call failures bringing down the entire process.

    PUBLIC METHODS:
        get_threshold_pairs() -> List[(symbol: str, rate_pct: float)]
        get_upcoming_pairs(window_sec: int) -> List[(symbol: str, rate_pct: float)]
        get_recent_pairs(window_sec: int) -> List[(symbol: str, rate_pct: float)]
    """

    EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    PREMIUM_INDEX_URL  = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    FUNDING_RATE_URL   = 'https://fapi.binance.com/fapi/v1/fundingRate'

    # How long to cache exchangeInfo (in seconds)
    _CACHE_TTL = 30 * 60  # 30 minutes

    def __init__(self, threshold_pct: float = 0.15):
        """
        :param threshold_pct: funding rate threshold in percent (e.g. 0.15 for 0.15%)
        """
        if threshold_pct < 0:
            raise ValueError("threshold_pct must be non-negative")

        # Convert to decimal
        self.threshold = threshold_pct / 100.0

        # Use a persistent Session so we can reuse connections
        self._session = requests.Session()

        # Cache for perpetual symbols and funding intervals
        self._cached_symbols = None  # set of symbol strings
        self._cached_intervals = None  # dict: symbol -> interval hours
        self._last_info_ts = 0  # timestamp when exchangeInfo was last fetched

    def _request_with_retries(self, url: str, params: dict = None, max_retries: int = 3, timeout: float = 5.0):
        """
        Simple GET with small retry loop on ConnectionError or Timeout.
        """
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
                # HTTP errors such as 4xx or 5xx: no point retrying repeatedly
                print(f"{datetime.now().isoformat()}  ❌  HTTP error from {url}: {e}. Aborting this request.")
                raise
        # After all retries failed:
        raise requests.exceptions.ConnectionError(f"Failed to GET {url} after {max_retries} attempts.")

    def _refresh_exchange_info_if_needed(self):
        """
        Fetches /exchangeInfo and /fundingRate (for intervals) only if cache is older than TTL.
        Otherwise returns the cached symbols and intervals.
        """
        now_ts = time.time()
        if self._cached_symbols and (now_ts - self._last_info_ts) < FundingRateScanner._CACHE_TTL:
            return

        # 1) Fetch perpetual symbols
        data = self._request_with_retries(FundingRateScanner.EXCHANGE_INFO_URL, params=None)
        symbols = {
            s['symbol']
            for s in data.get('symbols', [])
            if s.get('contractType') == 'PERPETUAL'
        }
        self._cached_symbols = symbols

        # 2) Fetch funding intervals in one shot
        arr = self._request_with_retries(FundingRateScanner.FUNDING_RATE_URL, params={'limit': 1000})
        intervals = {e['symbol']: e.get('fundingInterval', 8) for e in arr}
        self._cached_intervals = intervals

        self._last_info_ts = now_ts

    def _get_perpetual_symbols(self) -> set:
        self._refresh_exchange_info_if_needed()
        return self._cached_symbols or set()

    def _get_intervals(self) -> dict:
        self._refresh_exchange_info_if_needed()
        return self._cached_intervals or {}

    def _get_premium_index(self) -> list:
        data = self._request_with_retries(FundingRateScanner.PREMIUM_INDEX_URL, params=None)
        # data is a list of dicts, each containing 'symbol', 'lastFundingRate', etc.
        return data

    def scan(self) -> list:
        """
        Returns a list of dict entries for symbols whose |funding rate| ≥ threshold.
        Each dict:
            {
                'symbol': str,
                'rate_pct': float,       # e.g. 0.15 means 0.15%
                'next_funding': datetime,
                'interval_h': int
            }
        """
        symbols = self._get_perpetual_symbols()
        intervals = self._get_intervals()

        premium = []
        try:
            premium = self._get_premium_index()
        except Exception as e:
            # Log and return empty if fetching premiumIndex fails
            print(f"{datetime.now().isoformat()}  ❌  Failed to fetch premiumIndex: {e}")
            return []

        results = []
        now_utc = datetime.now(timezone.utc)

        for entry in premium:
            sym = entry.get('symbol')
            if not sym or sym not in symbols:
                continue

            # Sometimes entry['lastFundingRate'] is None or missing, so fallback to entry['fundingRate']
            raw_rate = entry.get('lastFundingRate')
            if raw_rate is None:
                raw_rate = entry.get('fundingRate', 0)

            try:
                rate = float(raw_rate)
            except (ValueError, TypeError):
                continue

            if abs(rate) < self.threshold:
                continue

            ts_ms = entry.get('nextFundingTime')
            if ts_ms is None:
                continue

            next_ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            interval = intervals.get(sym, 8)

            results.append({
                'symbol': sym,
                'rate_pct': rate * 100,       # Convert to percentage form
                'next_funding': next_ts,
                'interval_h': interval
            })

        return results

    def get_threshold_pairs(self) -> list:
        """
        Returns a list of (symbol, rate_pct) tuples for coins whose |funding rate| ≥ threshold.
        """
        try:
            scan_results = self.scan()
            return [(r['symbol'], r['rate_pct']) for r in scan_results]
        except Exception as e:
            # If scan() itself raised some unexpected error, log and return empty list
            print(f"{datetime.now().isoformat()}  ❌  Error in get_threshold_pairs: {e}")
            return []

    def get_upcoming_pairs(self, window_sec: int) -> list:
        """
        Returns a list of (symbol, rate_pct) for symbols whose next funding occurs
        within the next `window_sec` seconds.
        """
        try:
            now = datetime.now(timezone.utc)
            upcoming = []
            for r in self.scan():
                delta = (r['next_funding'] - now).total_seconds()
                if 0 <= delta <= window_sec:
                    upcoming.append((r['symbol'], r['rate_pct']))
            return upcoming
        except Exception as e:
            print(f"{datetime.now().isoformat()}  ❌  Error in get_upcoming_pairs: {e}")
            return []

    def get_recent_pairs(self, window_sec: int) -> list:
        """
        Returns a list of (symbol, rate_pct) for symbols whose funding was claimed
        within the past `window_sec` seconds.

        Compute last funding time as:
            last_ts = next_funding - interval_h hours
        """
        try:
            now = datetime.now(timezone.utc)
            recent = []
            for r in self.scan():
                last_ts = r['next_funding'] - timedelta(hours=r['interval_h'])
                delta = (now - last_ts).total_seconds()
                if 0 <= delta <= window_sec:
                    recent.append((r['symbol'], r['rate_pct']))
            return recent
        except Exception as e:
            print(f"{datetime.now().isoformat()}  ❌  Error in get_recent_pairs: {e}")
            return []


if __name__ == '__main__':
    # Quick standalone test
    scanner = FundingRateScanner(threshold_pct=0.1)
    print("Threshold pairs:", scanner.get_threshold_pairs())
    print("Upcoming pairs:", scanner.get_upcoming_pairs(window_sec=300))
    print("Recent pairs:", scanner.get_recent_pairs(window_sec=10))
