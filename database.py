"""
database.py — SQLite persistence layer for SME IPO Tracker
"""
import sqlite3
import pandas as pd
from datetime import datetime
import os

# On Linux/cloud (Streamlit Community Cloud), use /tmp so it always has write access.
# On Windows (local), use the project folder.
if os.name == "nt":
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sme_ipos.db")
else:
    DB_PATH = "/tmp/sme_ipos.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create all tables on first run."""
    conn = get_conn()
    c = conn.cursor()

    c.executescript('''
        CREATE TABLE IF NOT EXISTS ipos (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name       TEXT    NOT NULL,
            exchange           TEXT    NOT NULL,   -- "NSE" or "BSE"
            nse_symbol         TEXT,
            bse_code           TEXT,
            detail_url         TEXT    UNIQUE,
            open_date          TEXT,
            close_date         TEXT,
            listing_date       TEXT,
            issue_price        REAL,
            listing_price      REAL,
            issue_type         TEXT,               -- "Fresh Issue" / "OFS" / "Fresh Issue + OFS"
            fresh_issue_size   TEXT,
            ofs_size           TEXT,
            total_issue_size   TEXT,
            detail_scraped     INTEGER DEFAULT 0,  -- 0=pending, 1=done
            created_at         TEXT    DEFAULT CURRENT_TIMESTAMP,
            updated_at         TEXT    DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_cache (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange        TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            current_price   REAL,
            prev_close      REAL,
            day_change_pct  REAL,
            week52_high     REAL,
            week52_low      REAL,
            fetched_at      TEXT,
            UNIQUE(exchange, symbol)
        );

        CREATE TABLE IF NOT EXISTS corporate_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            exchange     TEXT,
            symbol       TEXT,
            event_type   TEXT,
            event_date   TEXT,
            purpose      TEXT,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, exchange, event_type, event_date)
        );
    ''')
    conn.commit()
    conn.close()


# ── IPO helpers ────────────────────────────────────────────────────────────────

def upsert_ipo(data: dict):
    """Insert or update an IPO record keyed on detail_url."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM ipos WHERE detail_url = ?", (data.get("detail_url"),))
    existing = c.fetchone()

    data = {k: v for k, v in data.items() if v is not None or k == "detail_url"}

    if existing:
        set_clause = ", ".join(f"{k} = ?" for k in data if k != "detail_url")
        vals = [data[k] for k in data if k != "detail_url"]
        vals.append(data["detail_url"])
        c.execute(
            f"UPDATE ipos SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE detail_url = ?",
            vals,
        )
    else:
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        c.execute(f"INSERT INTO ipos ({cols}) VALUES ({placeholders})", list(data.values()))

    conn.commit()
    conn.close()


def get_all_ipos(exchange: str = None, min_listing_date: str = None) -> pd.DataFrame:
    conn = get_conn()
    query = "SELECT * FROM ipos WHERE listing_date IS NOT NULL"
    params = []
    if exchange:
        query += " AND exchange = ?"
        params.append(exchange)
    if min_listing_date:
        query += " AND listing_date >= ?"
        params.append(min_listing_date)
    query += " ORDER BY listing_date DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_unscraped_details(limit: int = 100) -> list:
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id, company_name, exchange, detail_url
           FROM ipos
           WHERE detail_scraped = 0 AND detail_url IS NOT NULL AND detail_url != ''
           ORDER BY listing_date DESC
           LIMIT ?""",
        (limit,),
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def mark_detail_scraped(detail_url: str):
    conn = get_conn()
    conn.execute(
        "UPDATE ipos SET detail_scraped = 1, updated_at = CURRENT_TIMESTAMP WHERE detail_url = ?",
        (detail_url,),
    )
    conn.commit()
    conn.close()


def get_symbols_for_price_update() -> list:
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT company_name, exchange, nse_symbol, bse_code,
                  issue_price, listing_price, listing_date
           FROM ipos
           WHERE listing_date <= date('now')
             AND (nse_symbol IS NOT NULL OR bse_code IS NOT NULL)"""
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def count_ipos() -> int:
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM ipos")
    n = c.fetchone()[0]
    conn.close()
    return n


# ── Price cache helpers ────────────────────────────────────────────────────────

def upsert_price(exchange: str, symbol: str, price_data: dict):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO price_cache
           (exchange, symbol, current_price, prev_close, day_change_pct,
            week52_high, week52_low, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            exchange,
            symbol,
            price_data.get("price"),
            price_data.get("prev_close"),
            price_data.get("pct_change"),
            price_data.get("week52_high"),
            price_data.get("week52_low"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()
    conn.close()


def get_price_cache() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM price_cache", conn)
    conn.close()
    return df


# ── Corporate events ──────────────────────────────────────────────────────────

def upsert_event(data: dict):
    conn = get_conn()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO corporate_events
               (company_name, exchange, symbol, event_type, event_date, purpose)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                data.get("company_name"),
                data.get("exchange"),
                data.get("symbol"),
                data.get("event_type"),
                data.get("event_date"),
                data.get("purpose"),
            ),
        )
        conn.commit()
    except Exception:
        pass
    conn.close()


def get_upcoming_events(days_ahead: int = 45) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """SELECT ce.company_name, ce.exchange, ce.symbol,
                  ce.event_type, ce.event_date, ce.purpose
           FROM corporate_events ce
           WHERE ce.event_date >= date('now')
             AND ce.event_date <= date('now', '+' || ? || ' days')
           ORDER BY ce.event_date ASC""",
        conn,
        params=[days_ahead],
    )
    conn.close()
    return df
