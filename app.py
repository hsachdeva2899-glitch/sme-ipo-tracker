"""
app.py — SME IPO Performance Tracker
=====================================
Tracks NSE Emerge + BSE SME IPOs: performance since listing, real-time prices,
upcoming corporate events, and fresh issue vs OFS breakdown.

Run:  streamlit run app.py
"""

import time
import threading
import logging
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import database as db
import scraper
import price_fetcher

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SME IPO Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Tighten dataframe cells */
[data-testid="stDataFrame"] td { font-size: 13px !important; }
[data-testid="stDataFrame"] th { font-size: 13px !important; font-weight: 700; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #1e2130;
    border-radius: 10px;
    padding: 12px 16px;
    border: 1px solid #2e3250;
}

/* Positive / negative badges used inside HTML tables */
.gain  { color: #00c853; font-weight: 600; }
.loss  { color: #ff1744; font-weight: 600; }
.badge-fresh { background:#1565c0; color:#fff; padding:2px 7px; border-radius:4px; font-size:11px; }
.badge-ofs   { background:#6a1b9a; color:#fff; padding:2px 7px; border-radius:4px; font-size:11px; }
.badge-mixed { background:#e65100; color:#fff; padding:2px 7px; border-radius:4px; font-size:11px; }
.badge-na    { background:#37474f; color:#ccc; padding:2px 7px; border-radius:4px; font-size:11px; }

div[data-testid="stSidebarContent"] { background: #131625; }
</style>
""", unsafe_allow_html=True)


# ── DB init (once per process) ────────────────────────────────────────────────
db.init_db()


# ── Session-state keys ────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "initial_scrape_done": False,
        "prices_fetched_at":   None,
        "events_fetched_at":   None,
        "scrape_log":          [],
        "refresh_counter":     0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ─────────────────────────────────────────────────────────────────────────────
# Data loading helpers (cached)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)   # 5-min cache for price data
def _load_combined() -> pd.DataFrame:
    """
    Join the ipos table with the price_cache table and compute all derived columns.
    """
    ipos   = db.get_all_ipos()
    prices = db.get_price_cache()

    if ipos.empty:
        return pd.DataFrame()

    # Build a lookup: (exchange, symbol) -> price row
    price_map = {}
    for _, pr in prices.iterrows():
        key = (pr["exchange"], pr["symbol"])
        price_map[key] = pr

    records = []
    for _, row in ipos.iterrows():
        sym = row.get("nse_symbol") or row.get("bse_code") or ""
        exch = row["exchange"]
        pkey = (exch, sym)
        pr   = price_map.get(pkey, {})

        issue_price   = row.get("issue_price")
        listing_price = row.get("listing_price")
        current_price = pr.get("current_price") if pr else None

        listing_gain = (
            round((listing_price - issue_price) / issue_price * 100, 2)
            if issue_price and listing_price and issue_price > 0
            else None
        )
        current_gain = (
            round((current_price - issue_price) / issue_price * 100, 2)
            if issue_price and current_price and issue_price > 0
            else None
        )

        it = row.get("issue_type") or ""
        if "fresh" in it.lower() and "ofs" in it.lower():
            issue_badge = "Mixed"
        elif "fresh" in it.lower():
            issue_badge = "Fresh Issue"
        elif "ofs" in it.lower() or "offer" in it.lower():
            issue_badge = "OFS"
        else:
            issue_badge = "—"

        records.append({
            "Company":            row["company_name"],
            "Exchange":           exch,
            "Symbol":             sym,
            "Listing Date":       row.get("listing_date"),
            "Issue Price (₹)":   issue_price,
            "Listing Price (₹)": listing_price,
            "CMP (₹)":           current_price,
            "Listing Gain %":    listing_gain,
            "Total Gain %":      current_gain,
            "52W High (₹)":     pr.get("week52_high") if pr else None,
            "52W Low (₹)":      pr.get("week52_low")  if pr else None,
            "Day Chg %":         pr.get("day_change_pct") if pr else None,
            "Issue Type":         issue_badge,
            "Fresh Issue Size":   row.get("fresh_issue_size"),
            "OFS Size":           row.get("ofs_size"),
            "Total Issue Size":   row.get("total_issue_size"),
            "Prices Last Updated": pr.get("fetched_at") if pr else None,
            "detail_url":         row.get("detail_url"),
        })

    df = pd.DataFrame(records)
    if not df.empty and "Listing Date" in df.columns:
        df["Listing Date"] = pd.to_datetime(df["Listing Date"], errors="coerce")
        df = df.sort_values("Listing Date", ascending=False).reset_index(drop=True)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _load_events() -> pd.DataFrame:
    return db.get_upcoming_events(days_ahead=60)


# ─────────────────────────────────────────────────────────────────────────────
# Background workers
# ─────────────────────────────────────────────────────────────────────────────

def _run_initial_scrape(status_placeholder):
    """Full scrape of both list pages + detail pages for new entries."""
    logs = []

    def log(msg):
        logs.append(msg)
        st.session_state["scrape_log"] = logs[-10:]

    log("📡 Fetching NSE Emerge IPO list …")
    n_nse = scraper.scrape_list_page("NSE")
    log(f"   ✅ NSE: {n_nse} records saved")

    log("📡 Fetching BSE SME IPO list …")
    n_bse = scraper.scrape_list_page("BSE")
    log(f"   ✅ BSE: {n_bse} records saved")

    pending = db.get_unscraped_details(limit=500)
    total_pending = len(pending)
    log(f"🔍 Fetching detail pages for {total_pending} IPOs (issue type, symbol) …")

    def detail_progress(done, total, name):
        log(f"   [{done}/{total}] {name}")

    scraper.scrape_pending_details(batch_size=500, progress_cb=detail_progress)
    log("✅ Detail scrape complete")

    log("💹 Fetching live prices …")
    n_prices = price_fetcher.refresh_all_prices()
    log(f"   ✅ {n_prices} prices updated")
    st.session_state["prices_fetched_at"] = datetime.now()

    log("📅 Fetching upcoming events …")
    symbols = db.get_symbols_for_price_update()
    price_fetcher.refresh_events(symbols)
    st.session_state["events_fetched_at"] = datetime.now()

    st.session_state["initial_scrape_done"] = True
    _load_combined.clear()
    _load_events.clear()
    log("🎉 All done! Dashboard is ready.")


def _background_price_refresh():
    """Refresh prices silently in background; called from auto-refresh tick."""
    try:
        price_fetcher.refresh_all_prices()
        st.session_state["prices_fetched_at"] = datetime.now()
        scraper.refresh_new_ipos()   # pick up any brand-new IPOs
        _load_combined.clear()
        _load_events.clear()
    except Exception as e:
        logger.warning(f"Background refresh error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pct_badge(val):
    if val is None:
        return "—"
    color = "#00c853" if val >= 0 else "#ff1744"
    arrow = "▲" if val >= 0 else "▼"
    return f'<span style="color:{color};font-weight:600">{arrow} {val:+.2f}%</span>'


def _issue_badge(it: str) -> str:
    if it == "Fresh Issue":
        return '<span class="badge-fresh">Fresh Issue</span>'
    elif it == "OFS":
        return '<span class="badge-ofs">OFS</span>'
    elif it == "Mixed":
        return '<span class="badge-mixed">Mixed</span>'
    return '<span class="badge-na">—</span>'


def _fmt_price(val):
    if val is None:
        return "—"
    return f"₹{val:,.2f}"


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        st.title("⚙️ Filters & Controls")
        st.markdown("---")

        exchange_filter = st.multiselect(
            "Exchange", ["NSE", "BSE"], default=["NSE", "BSE"]
        )

        date_options = {
            "Last 6 months":  (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
            "Last 1 year":    (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            "Last 2 years":   (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
            "Last 3 years":   (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%d"),
            "All time":       "2000-01-01",
        }
        date_label = st.selectbox("Listed Since", list(date_options.keys()), index=2)
        min_date = date_options[date_label]

        issue_type_filter = st.multiselect(
            "Issue Type",
            ["Fresh Issue", "OFS", "Mixed", "—"],
            default=["Fresh Issue", "OFS", "Mixed", "—"],
        )

        sort_by = st.selectbox(
            "Sort by",
            ["Listing Date ↓", "Total Gain % ↓", "Total Gain % ↑", "Listing Gain % ↓"],
        )

        st.markdown("---")
        auto_refresh = st.toggle("Auto-refresh prices (5 min)", value=True)

        st.markdown("---")
        st.markdown("### 🔄 Manual Refresh")
        if st.button("Refresh Prices Now", use_container_width=True):
            with st.spinner("Fetching live prices …"):
                price_fetcher.refresh_all_prices()
                st.session_state["prices_fetched_at"] = datetime.now()
                _load_combined.clear()
            st.success("Prices updated!")

        if st.button("Rescan for New IPOs", use_container_width=True):
            with st.spinner("Scanning Chittorgarh for new listings …"):
                added = scraper.refresh_new_ipos()
                scraper.scrape_pending_details(batch_size=50)
                _load_combined.clear()
            st.success(f"Scan done. {added} new entries found.")

        last = st.session_state.get("prices_fetched_at")
        if last:
            elapsed = int((datetime.now() - last).total_seconds() / 60)
            st.caption(f"Prices last refreshed: {elapsed} min ago")

    return {
        "exchanges":    exchange_filter,
        "min_date":     min_date,
        "issue_types":  issue_type_filter,
        "sort_by":      sort_by,
        "auto_refresh": auto_refresh,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main dashboard tabs
# ─────────────────────────────────────────────────────────────────────────────

def render_dashboard(df: pd.DataFrame, filters: dict):
    # ── Apply filters ─────────────────────────────────────────────────────────
    if filters["exchanges"]:
        df = df[df["Exchange"].isin(filters["exchanges"])]

    if filters["min_date"] and "Listing Date" in df.columns:
        df = df[df["Listing Date"] >= pd.Timestamp(filters["min_date"])]

    if filters["issue_types"]:
        df = df[df["Issue Type"].isin(filters["issue_types"])]

    sort_map = {
        "Listing Date ↓":   ("Listing Date",   False),
        "Total Gain % ↓":   ("Total Gain %",   False),
        "Total Gain % ↑":   ("Total Gain %",   True),
        "Listing Gain % ↓": ("Listing Gain %", False),
    }
    scol, sasc = sort_map.get(filters["sort_by"], ("Listing Date", False))
    if scol in df.columns:
        df = df.sort_values(scol, ascending=sasc, na_position="last").reset_index(drop=True)

    # ── KPI cards ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total IPOs Tracked", len(df))

    priced = df[df["CMP (₹)"].notna()]
    gainers = priced[priced["Total Gain %"] >= 0]
    losers  = priced[priced["Total Gain %"] < 0]
    c2.metric("Gainers 🟢", len(gainers))
    c3.metric("Losers 🔴",  len(losers))

    avg_gain = priced["Total Gain %"].mean() if not priced.empty else None
    c4.metric("Avg Total Gain", f"{avg_gain:+.1f}%" if avg_gain is not None else "—")

    avg_list = df["Listing Gain %"].mean() if not df.empty else None
    c5.metric("Avg Listing Gain", f"{avg_list:+.1f}%" if avg_list is not None else "—")

    st.markdown("---")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_main, tab_chart, tab_events, tab_detail = st.tabs([
        "📋 IPO Performance Table",
        "📊 Charts",
        "📅 Upcoming Events",
        "🔬 Company Detail",
    ])

    # ─ Tab 1: Main table ───────────────────────────────────────────────────────
    with tab_main:
        st.caption(
            f"Showing {len(df)} IPOs | "
            f"CMP from NSE/BSE public APIs | "
            f"Prices auto-refresh every 5 minutes"
        )

        if df.empty:
            st.info("No data yet. Data is loading in the background — please wait a moment.")
        else:
            # Build display table with coloured % columns
            display_df = df[[
                "Company", "Exchange", "Symbol", "Listing Date",
                "Issue Price (₹)", "Listing Price (₹)", "CMP (₹)",
                "Listing Gain %", "Total Gain %", "Day Chg %",
                "52W High (₹)", "52W Low (₹)",
                "Issue Type", "Fresh Issue Size", "OFS Size", "Total Issue Size",
                "Prices Last Updated",
            ]].copy()

            display_df["Listing Date"] = display_df["Listing Date"].dt.strftime("%d %b %Y").fillna("—")

            # Color-code numeric %% columns with Streamlit's built-in gradient
            def _pct_fmt(v):
                if pd.isna(v):
                    return "—"
                return f"{v:+.2f}%"

            display_df["Listing Gain %"] = display_df["Listing Gain %"].apply(_pct_fmt)
            display_df["Total Gain %"]   = display_df["Total Gain %"].apply(_pct_fmt)
            display_df["Day Chg %"]      = display_df["Day Chg %"].apply(_pct_fmt)
            display_df["Issue Price (₹)"]   = display_df["Issue Price (₹)"].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
            display_df["Listing Price (₹)"] = display_df["Listing Price (₹)"].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
            display_df["CMP (₹)"]           = display_df["CMP (₹)"].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
            display_df["52W High (₹)"]      = display_df["52W High (₹)"].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
            display_df["52W Low (₹)"]       = display_df["52W Low (₹)"].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
            display_df["Prices Last Updated"] = display_df["Prices Last Updated"].fillna("—")
            for col in ["Fresh Issue Size", "OFS Size", "Total Issue Size"]:
                display_df[col] = display_df[col].fillna("—")

            st.dataframe(
                display_df,
                use_container_width=True,
                height=620,
                hide_index=True,
            )

            # Download button
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Download as CSV",
                csv,
                "sme_ipo_tracker.csv",
                "text/csv",
                use_container_width=False,
            )

    # ─ Tab 2: Charts ───────────────────────────────────────────────────────────
    with tab_chart:
        priced2 = df[df["Total Gain %"].notna()].copy()

        if priced2.empty:
            st.info("Price data is loading — charts will appear once prices are fetched.")
        else:
            # Ensure numeric for plotting (strip any % formatting already applied)
            for _col in ["Total Gain %", "Listing Gain %"]:
                priced2[_col] = pd.to_numeric(
                    priced2[_col].astype(str)
                        .str.replace("%", "", regex=False)
                        .str.replace("+", "", regex=False)
                        .str.replace("—", "", regex=False),
                    errors="coerce",
                )

            ch1, ch2 = st.columns(2)

            with ch1:
                st.subheader("Total Return Distribution")
                fig = px.histogram(
                    priced2, x="Total Gain %", nbins=40,
                    color_discrete_sequence=["#1f77b4"],
                    labels={"Total Gain %": "Total Return from Issue Price (%)"},
                )
                fig.update_layout(
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="white", margin=dict(t=30, b=10),
                )
                fig.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.4)
                st.plotly_chart(fig, use_container_width=True)

            with ch2:
                st.subheader("Listing Gain vs Total Gain (by Issue Type)")
                fig2 = px.scatter(
                    priced2.dropna(subset=["Listing Gain %", "Total Gain %"]),
                    x="Listing Gain %", y="Total Gain %",
                    color="Issue Type", hover_name="Company",
                    color_discrete_map={
                        "Fresh Issue": "#1565c0",
                        "OFS":         "#6a1b9a",
                        "Mixed":       "#e65100",
                        "—":           "#546e7a",
                    },
                )
                fig2.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.3)
                fig2.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.3)
                fig2.update_layout(
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font_color="white", margin=dict(t=30, b=10),
                )
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Top 20 Gainers & Losers (Total Return from Issue Price)")
            top20 = pd.concat([
                priced2.nlargest(10, "Total Gain %"),
                priced2.nsmallest(10, "Total Gain %"),
            ]).drop_duplicates()

            fig3 = px.bar(
                top20.sort_values("Total Gain %"),
                x="Total Gain %", y="Company",
                orientation="h",
                color="Total Gain %",
                color_continuous_scale=["#ff1744", "#b71c1c", "#1b5e20", "#00c853"],
                color_continuous_midpoint=0,
                hover_data=["Exchange", "Listing Date", "Issue Type"],
                text="Total Gain %",
            )
            fig3.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig3.update_layout(
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                font_color="white", height=600,
                margin=dict(t=20, b=10), showlegend=False,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig3, use_container_width=True)

            # Exchange breakdown pie
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("IPOs by Exchange")
                exc_counts = df["Exchange"].value_counts().reset_index()
                exc_counts.columns = ["Exchange", "Count"]
                fig4 = px.pie(exc_counts, names="Exchange", values="Count",
                              color_discrete_sequence=["#1f77b4","#ff7f0e"])
                fig4.update_layout(paper_bgcolor="#0e1117", font_color="white", margin=dict(t=20))
                st.plotly_chart(fig4, use_container_width=True)

            with col_b:
                st.subheader("IPOs by Issue Type")
                it_counts = df["Issue Type"].value_counts().reset_index()
                it_counts.columns = ["Issue Type", "Count"]
                fig5 = px.pie(it_counts, names="Issue Type", values="Count",
                              color_discrete_sequence=["#1565c0","#6a1b9a","#e65100","#546e7a"])
                fig5.update_layout(paper_bgcolor="#0e1117", font_color="white", margin=dict(t=20))
                st.plotly_chart(fig5, use_container_width=True)

    # ─ Tab 3: Upcoming events ──────────────────────────────────────────────────
    with tab_events:
        events_df = _load_events()
        st.subheader("Upcoming Corporate Events (next 60 days)")
        if events_df.empty:
            st.info(
                "No upcoming events found yet. Events are fetched from NSE/BSE "
                "corporate action APIs and updated with each price refresh."
            )
        else:
            st.dataframe(events_df, use_container_width=True, hide_index=True)

    # ─ Tab 4: Company detail ───────────────────────────────────────────────────
    with tab_detail:
        st.subheader("Company Deep-Dive")
        companies = df["Company"].dropna().unique().tolist()
        if not companies:
            st.info("No data loaded yet.")
        else:
            selected = st.selectbox("Select a company", companies)
            row = df[df["Company"] == selected].iloc[0] if not df[df["Company"] == selected].empty else None

            if row is not None:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Issue Price",   _fmt_price(row.get("Issue Price (₹)")))
                    st.metric("Listing Price", _fmt_price(row.get("Listing Price (₹)")))
                    st.metric("CMP",           _fmt_price(row.get("CMP (₹)")))
                with col2:
                    st.metric("Listing Gain", row.get("Listing Gain %", "—"))
                    st.metric("Total Gain",   row.get("Total Gain %", "—"))
                    st.metric("Day Change",   row.get("Day Chg %", "—"))
                with col3:
                    st.metric("52W High", _fmt_price(row.get("52W High (₹)")))
                    st.metric("52W Low",  _fmt_price(row.get("52W Low (₹)")))
                    st.metric("Exchange", row.get("Exchange", "—"))

                st.markdown(f"**Issue Type:** {row.get('Issue Type', '—')}")
                if row.get("Fresh Issue Size") and row.get("Fresh Issue Size") != "—":
                    st.markdown(f"**Fresh Issue Size:** {row.get('Fresh Issue Size')}")
                if row.get("OFS Size") and row.get("OFS Size") != "—":
                    st.markdown(f"**OFS Size:** {row.get('OFS Size')}")
                if row.get("Total Issue Size") and row.get("Total Issue Size") != "—":
                    st.markdown(f"**Total Issue Size:** {row.get('Total Issue Size')}")

                detail_url = row.get("detail_url")
                if detail_url:
                    st.markdown(f"[📄 View on Chittorgarh]({detail_url})")


# ─────────────────────────────────────────────────────────────────────────────
# First-run initialisation modal
# ─────────────────────────────────────────────────────────────────────────────

def render_first_run():
    st.title("📈 SME IPO Tracker")
    st.info(
        "**First-time setup** — Loading 2–3 years of SME IPO data from Chittorgarh "
        "and fetching live prices from NSE/BSE. This takes 3–8 minutes (once only). "
        "Subsequent opens are instant."
    )
    placeholder = st.empty()
    log_box     = st.empty()

    if st.button("🚀 Start Loading Data", type="primary", use_container_width=True):
        with st.spinner(""):
            _run_initial_scrape(placeholder)
        st.success("Setup complete! Reload the page to view the dashboard.")
        time.sleep(1)
        st.rerun()

    # Show log
    if st.session_state.get("scrape_log"):
        with log_box.container():
            st.code("\n".join(st.session_state["scrape_log"]))


# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh ticker
# ─────────────────────────────────────────────────────────────────────────────

AUTO_REFRESH_SECS = 300  # 5 minutes


def _maybe_auto_refresh(auto_refresh: bool):
    last = st.session_state.get("prices_fetched_at")
    if not auto_refresh or not st.session_state.get("initial_scrape_done"):
        return
    if last is None or (datetime.now() - last).total_seconds() >= AUTO_REFRESH_SECS:
        with st.spinner("Auto-refreshing prices …"):
            _background_price_refresh()
        st.session_state["refresh_counter"] += 1


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Check if we have data in DB
    n = db.count_ipos()
    initial_done = n > 0 or st.session_state.get("initial_scrape_done", False)

    if not initial_done:
        render_first_run()
        return

    st.session_state["initial_scrape_done"] = True
    filters = render_sidebar()

    # Auto-refresh check
    _maybe_auto_refresh(filters.get("auto_refresh", True))

    # Header
    last_refresh = st.session_state.get("prices_fetched_at")
    last_str = last_refresh.strftime("%d %b %Y %H:%M") if last_refresh else "—"
    st.title("📈 SME IPO Tracker")
    st.caption(
        f"NSE Emerge + BSE SME | Real-time prices via NSE/BSE APIs | "
        f"Last price update: {last_str} | "
        f"Auto-refreshes every 5 minutes"
    )

    # Load data
    df = _load_combined()
    render_dashboard(df, filters)

    # Schedule next auto-refresh via a meta-refresh trick
    if filters.get("auto_refresh"):
        countdown = AUTO_REFRESH_SECS
        if last_refresh:
            elapsed = (datetime.now() - last_refresh).total_seconds()
            countdown = max(30, int(AUTO_REFRESH_SECS - elapsed))
        st.markdown(
            f"""<meta http-equiv="refresh" content="{countdown}">""",
            unsafe_allow_html=True,
        )
        st.caption(f"⏱️ Next auto-refresh in ~{countdown//60}m {countdown%60}s")


if __name__ == "__main__":
    main()
