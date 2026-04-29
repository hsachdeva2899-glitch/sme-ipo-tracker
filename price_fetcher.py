"""
price_fetcher.py — Real-time quotes from NSE and BSE public APIs.

NSE endpoint  : https://www.nseindia.com/api/quote-equity?symbol=SYMBOL
BSE endpoint  : https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w?scripcode=CODE

Both are the same APIs that power nseindia.com and bseindia.com themselves.
No paid subscription needed; a valid browser-like session is sufficient.
"""

import time
import logging
from datetime import datetime, timedelta

import requests

import database as db

logger = logging.getLogger(__name__)

# ── NSE session (needs homepage cookies) ──────────────────────────────────────

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
}


class NSEClient:
    """Manages a single requests.Session for NSE API calls."""

    def __init__(self):
        self._session = None
        self._last_init = None
        self._session_ttl = timedelta(minutes=20)

    def _init(self):
        s = requests.Session()
        s.headers.update(NSE_HEADERS)
        try:
            s.get("https://www.nseindia.com", timeout=12)
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"NSE session init warning: {e}")
        self._session = s
        self._last_init = datetime.now()

    def _get_session(self) -> requests.Session:
        if (
            self._session is None
            or self._last_init is None
            or datetime.now() - self._last_init > self._session_ttl
        ):
            self._init()
        return self._session

    def get_quote(self, symbol: str):
        """Return a dict with price fields, or None on failure."""
        s = self._get_session()
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol.upper()}"
        try:
            resp = s.get(url, timeout=12)
            if resp.status_code == 401:
                # Cookie expired — reinitialise once and retry
                self._init()
                resp = self._session.get(url, timeout=12)
            if resp.status_code != 200:
                return None
            data = resp.json()
            pi = data.get("priceInfo", {})
            whl = pi.get("weekHighLow", {})
            idhl = pi.get("intraDayHighLow", {})
            return {
                "price":        _safe_float(pi.get("lastPrice")),
                "prev_close":   _safe_float(pi.get("previousClose")),
                "pct_change":   _safe_float(pi.get("pChange")),
                "day_high":     _safe_float(idhl.get("max")),
                "day_low":      _safe_float(idhl.get("min")),
                "week52_high":  _safe_float(whl.get("max")),
                "week52_low":   _safe_float(whl.get("min")),
            }
        except Exception as e:
            logger.error(f"NSE quote error [{symbol}]: {e}")
            return None

    def get_corporate_actions(self, symbol: str) -> list:
        """Return upcoming corporate actions (board meetings, dividends, results)."""
        s = self._get_session()
        url = (
            f"https://www.nseindia.com/api/corporates-corporateActions"
            f"?index=equities&symbol={symbol.upper()}"
        )
        events = []
        try:
            resp = s.get(url, timeout=12)
            if resp.status_code != 200:
                return events
            for item in resp.json():
                ed = item.get("exDate") or item.get("bcStartDate") or ""
                events.append({
                    "symbol":     symbol.upper(),
                    "exchange":   "NSE",
                    "event_type": item.get("series", item.get("purpose", "EVENT")),
                    "event_date": _normalise_date(ed),
                    "purpose":    item.get("purpose", ""),
                })
        except Exception as e:
            logger.warning(f"NSE corp-actions error [{symbol}]: {e}")
        return events


class BSEClient:
    """Lightweight BSE quote fetcher (stateless – no cookie needed)."""

    def get_quote(self, bse_code: str):
        url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w"
            f"?Debtflag=&scripcode={bse_code}&seriesid="
        )
        try:
            resp = requests.get(url, headers=BSE_HEADERS, timeout=12)
            if resp.status_code != 200:
                return None
            data = resp.json()
            return {
                "price":       _safe_float(data.get("CurrRate")),
                "prev_close":  _safe_float(data.get("PrevClose")),
                "pct_change":  _safe_float(data.get("PercentChange")),
                "week52_high": _safe_float(data.get("High52Week")),
                "week52_low":  _safe_float(data.get("Low52Week")),
            }
        except Exception as e:
            logger.error(f"BSE quote error [{bse_code}]: {e}")
            return None

    def get_corporate_actions(self, bse_code: str, company_name: str = "") -> list:
        from datetime import date
        today = date.today().strftime("%Y%m%d")
        future = (date.today() + timedelta(days=90)).strftime("%Y%m%d")
        url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/CorporateActions/w"
            f"?scripcode={bse_code}&From={today}&To={future}&type=C"
        )
        events = []
        try:
            resp = requests.get(url, headers=BSE_HEADERS, timeout=12)
            if resp.status_code != 200:
                return events
            for item in resp.json().get("Table", []):
                events.append({
                    "company_name": company_name or item.get("short_name", ""),
                    "symbol":       bse_code,
                    "exchange":     "BSE",
                    "event_type":   item.get("purpose", "EVENT"),
                    "event_date":   _normalise_date(item.get("Ex_date", "")),
                    "purpose":      item.get("details", ""),
                })
        except Exception as e:
            logger.warning(f"BSE corp-actions error [{bse_code}]: {e}")
        return events


# ── Batch price refresh ───────────────────────────────────────────────────────

_nse_client = NSEClient()
_bse_client = BSEClient()


def refresh_all_prices(progress_cb=None) -> int:
    """
    Fetch latest prices for every listed IPO that has a symbol/code.
    Updates the price_cache table.  Returns count of successful fetches.
    """
    rows = db.get_symbols_for_price_update()
    total = len(rows)
    updated = 0

    for i, row in enumerate(rows):
        exchange   = row["exchange"]
        nse_symbol = row.get("nse_symbol")
        bse_code   = row.get("bse_code")

        quote = None
        if exchange == "NSE" and nse_symbol:
            quote = _nse_client.get_quote(nse_symbol)
            if quote:
                db.upsert_price("NSE", nse_symbol, quote)
        elif exchange == "BSE" and bse_code:
            quote = _bse_client.get_quote(bse_code)
            if quote:
                db.upsert_price("BSE", bse_code, quote)
        # Some IPOs are listed on both — try the other if primary failed
        if not quote and nse_symbol and exchange == "BSE":
            quote = _nse_client.get_quote(nse_symbol)
            if quote:
                db.upsert_price("NSE", nse_symbol, quote)

        if quote:
            updated += 1

        time.sleep(0.15)  # ~7 req/s — well within limits

        if progress_cb:
            progress_cb(i + 1, total)

    return updated


def refresh_events(symbols: list[dict], progress_cb=None):
    """Fetch upcoming corporate events for a list of {exchange, symbol, bse_code, company_name} dicts."""
    for i, row in enumerate(symbols):
        exchange   = row.get("exchange")
        nse_symbol = row.get("nse_symbol")
        bse_code   = row.get("bse_code")
        company    = row.get("company_name", "")

        if exchange == "NSE" and nse_symbol:
            for ev in _nse_client.get_corporate_actions(nse_symbol):
                ev["company_name"] = company
                if ev.get("event_date"):
                    db.upsert_event(ev)
        if bse_code:
            for ev in _bse_client.get_corporate_actions(bse_code, company):
                if ev.get("event_date"):
                    db.upsert_event(ev)

        time.sleep(0.2)
        if progress_cb:
            progress_cb(i + 1, len(symbols))


# ── Utilities ─────────────────────────────────────────────────────────────────

def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalise_date(s: str):
    """Convert 'DD-MMM-YYYY' or 'YYYY-MM-DD' to 'YYYY-MM-DD'."""
    if not s:
        return None
    from datetime import datetime as dt
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d %b %Y", "%d-%m-%Y"):
        try:
            return dt.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None
