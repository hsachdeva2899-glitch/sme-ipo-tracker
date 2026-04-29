"""
scraper.py — Scrapes SME IPO data from Chittorgarh (NSE Emerge + BSE SME)

List pages:
  NSE: https://www.chittorgarh.com/report/nse-sme-ipo-list-nse-emerge-ipo-listing-gain/106/
  BSE: https://www.chittorgarh.com/report/bse-sme-ipo-list-bse-sme-ipo-listing-gain/107/

Detail pages (per IPO):
  https://www.chittorgarh.com/ipo/<slug>/<id>/
  These carry: Fresh Issue size, OFS size, issue type, NSE symbol / BSE code.
"""

import re
import time
import logging
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

import database as db

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.chittorgarh.com/",
}

LIST_URLS = {
    "NSE": "https://www.chittorgarh.com/report/nse-sme-ipo-list-nse-emerge-ipo-listing-gain/106/",
    "BSE": "https://www.chittorgarh.com/report/bse-sme-ipo-list-bse-sme-ipo-listing-gain/107/",
}

BASE_URL = "https://www.chittorgarh.com"

# Only scrape IPOs listed on/after this date
CUTOFF_DATE = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _parse_price(text: str):
    """Extract float from strings like '₹123.45' or '123.45'."""
    text = _clean(text)
    text = text.replace("₹", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(text: str) -> str:
    """Normalise various date formats to YYYY-MM-DD."""
    text = _clean(text)
    fmts = ["%d %b %Y", "%d-%b-%Y", "%b %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d %B %Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _abs_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return BASE_URL + href


# ── List-page scraper ─────────────────────────────────────────────────────────

def scrape_list_page(exchange: str, progress_cb=None) -> int:
    """
    Scrape the Chittorgarh SME IPO list for the given exchange.
    Upserts rows into the DB. Returns count of new/updated rows.
    """
    url = LIST_URLS[exchange]
    session = _session()
    saved = 0

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"[{exchange}] Failed to fetch list page: {e}")
        return 0

    soup = BeautifulSoup(resp.text, "lxml")

    # Chittorgarh renders IPO tables inside <table class="table ...">
    table = soup.find("table")
    if not table:
        logger.error(f"[{exchange}] No table found on list page.")
        return 0

    # Parse header to know column positions
    thead = table.find("thead")
    header_cells = []
    if thead:
        header_cells = [_clean(th.get_text()) for th in thead.find_all(["th", "td"])]

    def col_idx(*candidates):
        """Return index of first matching header (case-insensitive)."""
        for candidate in candidates:
            for i, h in enumerate(header_cells):
                if candidate.lower() in h.lower():
                    return i
        return None

    idx_company   = col_idx("company", "ipo name") or 0
    idx_open      = col_idx("open date", "open")
    idx_close     = col_idx("close date", "close")
    idx_listing   = col_idx("listing date", "list date", "listing")
    idx_issue_px  = col_idx("issue price", "price")
    idx_list_px   = col_idx("listing price", "list price")

    tbody = table.find("tbody") or table
    rows = tbody.find_all("tr")
    total = len(rows)

    for i, tr in enumerate(rows):
        cells = tr.find_all("td")
        if not cells:
            continue

        def cell(idx):
            if idx is None or idx >= len(cells):
                return ""
            return _clean(cells[idx].get_text())

        # Company name + detail URL
        company_cell = cells[idx_company] if idx_company < len(cells) else cells[0]
        link = company_cell.find("a")
        company_name = _clean(company_cell.get_text())
        detail_url = _abs_url(link["href"]) if link and link.get("href") else ""

        listing_date = _parse_date(cell(idx_listing))

        # Skip if too old
        if listing_date and listing_date < CUTOFF_DATE:
            continue
        # Skip if not yet listed (future IPOs without listing date are kept)

        record = {
            "company_name":  company_name,
            "exchange":      exchange,
            "detail_url":    detail_url,
            "open_date":     _parse_date(cell(idx_open)),
            "close_date":    _parse_date(cell(idx_close)),
            "listing_date":  listing_date,
            "issue_price":   _parse_price(cell(idx_issue_px)),
            "listing_price": _parse_price(cell(idx_list_px)),
        }

        db.upsert_ipo(record)
        saved += 1

        if progress_cb:
            progress_cb(i + 1, total)

    return saved


# ── Detail-page scraper ───────────────────────────────────────────────────────

def scrape_detail_page(detail_url: str) -> dict:
    """
    Fetch the individual IPO page and extract:
      - issue_type      (Fresh Issue / OFS / Fresh Issue + OFS)
      - fresh_issue_size
      - ofs_size
      - total_issue_size
      - nse_symbol
      - bse_code
    """
    detail = {
        "issue_type": None,
        "fresh_issue_size": None,
        "ofs_size": None,
        "total_issue_size": None,
        "nse_symbol": None,
        "bse_code": None,
    }

    try:
        session = _session()
        time.sleep(0.6)  # polite delay
        resp = session.get(detail_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Detail fetch failed for {detail_url}: {e}")
        return detail

    soup = BeautifulSoup(resp.text, "lxml")

    # Strategy 1 – scan ALL table rows for key–value pairs
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        key   = _clean(cells[0].get_text()).lower()
        value = _clean(cells[1].get_text())

        if not value or value in ["-", "N/A", "NA", "Nil"]:
            continue

        if "fresh issue" in key:
            detail["fresh_issue_size"] = value
        elif "offer for sale" in key or "ofs" in key:
            detail["ofs_size"] = value
        elif re.search(r"(total\s+)?issue\s+size", key):
            detail["total_issue_size"] = value
        elif "issue type" in key:
            detail["issue_type"] = value
        elif "nse symbol" in key or "nse emerge" in key or "nse scrip" in key:
            sym = re.sub(r"[^A-Z0-9]", "", value.upper())
            if sym:
                detail["nse_symbol"] = sym
        elif "bse code" in key or "bse scrip" in key or "bse script" in key:
            code = re.sub(r"\D", "", value)
            if code:
                detail["bse_code"] = code

    # Strategy 2 – definition lists
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            key   = _clean(dt.get_text()).lower()
            value = _clean(dd.get_text())
            if "fresh issue" in key:
                detail["fresh_issue_size"] = value
            elif "offer for sale" in key or "ofs" in key:
                detail["ofs_size"] = value

    # Strategy 3 – scan visible text for "NSE Symbol: XYZ" patterns
    page_text = soup.get_text(separator=" ")
    m = re.search(r"NSE\s+(?:Symbol|Emerge)\s*[:\-]\s*([A-Z0-9]{3,20})", page_text, re.IGNORECASE)
    if m and not detail["nse_symbol"]:
        detail["nse_symbol"] = m.group(1).upper()

    m = re.search(r"BSE\s+(?:Code|Script)\s*[:\-]\s*(\d{5,6})", page_text, re.IGNORECASE)
    if m and not detail["bse_code"]:
        detail["bse_code"] = m.group(1)

    # Derive issue_type from sizes if not explicitly found
    if not detail["issue_type"]:
        nil_values = {"", "0", "₹0", "nil", "n/a", "-"}
        has_fresh = (detail["fresh_issue_size"] or "").lower().replace(" ", "") not in nil_values and bool(detail["fresh_issue_size"])
        has_ofs   = (detail["ofs_size"] or "").lower().replace(" ", "") not in nil_values and bool(detail["ofs_size"])
        if has_fresh and has_ofs:
            detail["issue_type"] = "Fresh Issue + OFS"
        elif has_fresh:
            detail["issue_type"] = "Fresh Issue"
        elif has_ofs:
            detail["issue_type"] = "OFS"

    return detail


# ── Batch detail scraper ──────────────────────────────────────────────────────

def scrape_pending_details(batch_size: int = 50, progress_cb=None):
    """
    Fetch detail pages for IPOs where detail_scraped == 0.
    Updates the DB in place. Runs at most `batch_size` per call.
    """
    pending = db.get_unscraped_details(limit=batch_size)
    total   = len(pending)

    for i, row in enumerate(pending):
        url = row["detail_url"]
        if not url:
            db.mark_detail_scraped(url or "")
            continue

        detail = scrape_detail_page(url)
        detail["detail_scraped"] = 1
        detail["detail_url"] = url
        db.upsert_ipo(detail)

        if progress_cb:
            progress_cb(i + 1, total, row["company_name"])

    return total


# ── New-IPO watcher ───────────────────────────────────────────────────────────

def refresh_new_ipos():
    """
    Light refresh: re-scrape the list page to pick up any IPOs
    added in the past 60 days. Called on every app open.
    """
    added = 0
    for exchange in ("NSE", "BSE"):
        added += scrape_list_page(exchange)
    return added
