"""
IPO Performance Tracker
========================
Tracks Mainboard + SME IPO performance with live prices from yfinance.
Data source: data/ipos.csv (seeded from your Excel, symbols auto-mapped to NSE)
"""

import warnings
warnings.filterwarnings("ignore")

import time
from datetime import datetime

import pandas as pd
import yfinance as yf
import streamlit as st
import plotly.express as px

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPO Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="metric-container"] {
    background: #1e2130; border-radius: 10px;
    padding: 10px 16px; border: 1px solid #2e3250;
}
div[data-testid="stSidebarContent"] { background: #131625; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
DATA_FILE      = "data/ipos.csv"
REFRESH_SECS   = 300   # 5 minutes

# ── Load base data (fast — just a CSV read) ───────────────────────────────────
@st.cache_data(show_spinner=False)
def load_base() -> pd.DataFrame:
    df = pd.read_csv(DATA_FILE)
    df["Listing Date"] = pd.to_datetime(df["Listing Date"], errors="coerce")
    df["Issue Price"]        = pd.to_numeric(df["Issue Price"],        errors="coerce")
    df["Listing Day Close"]  = pd.to_numeric(df["Listing Day Close"],  errors="coerce")
    df["Listing Day Gain"]   = pd.to_numeric(df["Listing Day Gain"],   errors="coerce")
    return df


# ── Fetch live prices from yfinance ──────────────────────────────────────────
@st.cache_data(ttl=REFRESH_SECS, show_spinner=False)
def fetch_prices(symbols: tuple) -> dict:
    """
    Returns {symbol: {price, prev_close, pct_change, week52_high, week52_low}}
    Batch-downloads via yfinance for speed.
    """
    valid = [s for s in symbols if s]
    if not valid:
        return {}

    tickers = [f"{s}.NS" for s in valid]
    prices  = {}

    try:
        data = yf.download(
            tickers,
            period="1y",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        for sym, ticker in zip(valid, tickers):
            try:
                if len(valid) == 1:
                    # Single ticker — yfinance returns flat DataFrame
                    close = data["Close"].dropna()
                else:
                    close = data[ticker]["Close"].dropna()

                if len(close) < 2:
                    continue

                cur       = float(close.iloc[-1])
                prev      = float(close.iloc[-2])
                pct_chg   = round((cur - prev) / prev * 100, 2) if prev else None
                high_52w  = float(close.tail(252).max())
                low_52w   = float(close.tail(252).min())

                prices[sym] = {
                    "price":       round(cur, 2),
                    "prev_close":  round(prev, 2),
                    "pct_change":  pct_chg,
                    "week52_high": round(high_52w, 2),
                    "week52_low":  round(low_52w, 2),
                }
            except Exception:
                continue
    except Exception as e:
        st.warning(f"Price fetch warning: {e}")

    return prices


# ── Build display DataFrame ───────────────────────────────────────────────────
def build_display(base: pd.DataFrame, prices: dict) -> pd.DataFrame:
    rows = []
    for _, r in base.iterrows():
        sym          = r.get("NSE Symbol", "")
        issue_price  = r.get("Issue Price")
        list_close   = r.get("Listing Day Close")
        list_gain    = r.get("Listing Day Gain")
        pr           = prices.get(sym, {}) if sym else {}
        cur_price    = pr.get("price")

        total_gain = (
            round((cur_price - issue_price) / issue_price * 100, 2)
            if issue_price and cur_price and issue_price > 0
            else None
        )

        rows.append({
            "Company":          r["Company Name"],
            "Type":             r.get("Sheet", ""),
            "Business":         r.get("Business", ""),
            "Listed":           r["Listing Date"],
            "Issue ₹":          issue_price,
            "List Close ₹":    list_close,
            "List Gain %":     list_gain,
            "CMP ₹":           cur_price,
            "Total Gain %":    total_gain,
            "Day Chg %":       pr.get("pct_change"),
            "52W High ₹":     pr.get("week52_high"),
            "52W Low ₹":      pr.get("week52_low"),
            "Symbol":          sym,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Listed", ascending=False, na_position="last")
    return df.reset_index(drop=True)


# ── Formatting helpers ────────────────────────────────────────────────────────
def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    arrow = "▲" if v >= 0 else "▼"
    return f"{arrow} {v:+.2f}%"

def fmt_price(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"₹{v:,.2f}"

def color_pct(v):
    """Return CSS color string for a percentage value."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return "color: #00c853; font-weight:600" if v >= 0 else "color: #ff1744; font-weight:600"


# ── Sidebar ───────────────────────────────────────────────────────────────────
def sidebar():
    with st.sidebar:
        st.title("⚙️ Filters")

        ipo_type = st.multiselect(
            "IPO Type", ["Mainboard", "SME"], default=["Mainboard", "SME"]
        )

        date_opts = {
            "Last 6 months":  180,
            "Last 1 year":    365,
            "Last 2 years":   730,
            "Last 3 years":   1095,
            "All":            9999,
        }
        date_sel = st.selectbox("Listed Since", list(date_opts.keys()), index=1)
        days_back = date_opts[date_sel]

        sort_opts = {
            "Listed (newest first)":   ("Listed",       False),
            "Total Gain % ↓ (best)":  ("Total Gain %", False),
            "Total Gain % ↑ (worst)": ("Total Gain %", True),
            "List Gain % ↓":          ("List Gain %",  False),
        }
        sort_sel  = st.selectbox("Sort by", list(sort_opts.keys()))
        sort_col, sort_asc = sort_opts[sort_sel]

        st.markdown("---")
        st.caption("Prices refresh automatically every 5 min.\nYou can also force-refresh:")
        if st.button("🔄 Refresh Prices Now", use_container_width=True):
            fetch_prices.clear()
            st.rerun()

    return ipo_type, days_back, sort_col, sort_asc


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ipo_type, days_back, sort_col, sort_asc = sidebar()

    st.title("📈 IPO Performance Tracker")
    st.caption(
        "Mainboard + SME IPOs | Live prices via NSE (yfinance) | "
        f"Auto-refreshes every 5 min | {datetime.now().strftime('%d %b %Y %H:%M')}"
    )

    # ── Load + price ─────────────────────────────────────────────────────────
    base   = load_base()
    syms   = tuple(base["NSE Symbol"].dropna().unique().tolist())

    with st.spinner("Fetching live prices from NSE…"):
        prices = fetch_prices(syms)

    df = build_display(base, prices)

    # ── Apply filters ─────────────────────────────────────────────────────────
    if ipo_type:
        df = df[df["Type"].isin(ipo_type)]

    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_back)
    df = df[df["Listed"].isna() | (df["Listed"] >= cutoff)]

    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=sort_asc, na_position="last")

    # ── KPI cards ─────────────────────────────────────────────────────────────
    priced = df[df["Total Gain %"].notna()]
    gainers = priced[priced["Total Gain %"] >= 0]
    losers  = priced[priced["Total Gain %"] <  0]
    avg_tot = priced["Total Gain %"].mean() if not priced.empty else None
    avg_lst = df["List Gain %"].dropna().mean()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total IPOs",      len(df))
    c2.metric("🟢 Gainers",       len(gainers))
    c3.metric("🔴 Losers",        len(losers))
    c4.metric("Avg Total Gain",  f"{avg_tot:+.1f}%" if avg_tot is not None else "—")
    c5.metric("Avg Listing Gain", f"{avg_lst:+.1f}%" if pd.notna(avg_lst) else "—")

    st.markdown("---")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_table, tab_charts, tab_detail = st.tabs([
        "📋 Performance Table", "📊 Charts", "🔍 Company Detail"
    ])

    # ── Tab 1: Table ──────────────────────────────────────────────────────────
    with tab_table:
        st.caption(
            f"Showing {len(df)} IPOs | "
            f"Live prices for {len(priced)} companies | "
            f"{len(df) - len(priced)} without symbol match yet"
        )

        # Build styled display table
        disp = df[[
            "Company","Type","Listed","Issue ₹","List Close ₹",
            "List Gain %","CMP ₹","Total Gain %","Day Chg %",
            "52W High ₹","52W Low ₹","Symbol","Business"
        ]].copy()

        disp["Listed"]       = disp["Listed"].dt.strftime("%d %b %Y").fillna("—")
        disp["Issue ₹"]      = disp["Issue ₹"].apply(fmt_price)
        disp["List Close ₹"] = disp["List Close ₹"].apply(fmt_price)
        disp["CMP ₹"]        = disp["CMP ₹"].apply(fmt_price)
        disp["52W High ₹"]   = disp["52W High ₹"].apply(fmt_price)
        disp["52W Low ₹"]    = disp["52W Low ₹"].apply(fmt_price)
        disp["List Gain %"]  = disp["List Gain %"].apply(fmt_pct)
        disp["Total Gain %"] = disp["Total Gain %"].apply(fmt_pct)
        disp["Day Chg %"]    = disp["Day Chg %"].apply(fmt_pct)
        disp["Business"]     = disp["Business"].fillna("").str[:120]

        st.dataframe(disp, use_container_width=True, height=620, hide_index=True)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", csv, "ipo_tracker.csv", "text/csv")

    # ── Tab 2: Charts ─────────────────────────────────────────────────────────
    with tab_charts:
        if priced.empty:
            st.info("No price data yet — charts will appear once prices load.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Total Return Distribution")
                fig = px.histogram(
                    priced, x="Total Gain %", nbins=40,
                    color="Type",
                    color_discrete_map={"Mainboard": "#1f77b4", "SME": "#ff7f0e"},
                    barmode="overlay", opacity=0.75,
                )
                fig.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
                fig.update_layout(paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                                  font_color="white", margin=dict(t=30,b=10))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("Listing Gain vs Total Gain")
                fig2 = px.scatter(
                    priced.dropna(subset=["List Gain %", "Total Gain %"]),
                    x="List Gain %", y="Total Gain %",
                    color="Type", hover_name="Company",
                    color_discrete_map={"Mainboard": "#1f77b4", "SME": "#ff7f0e"},
                )
                fig2.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.3)
                fig2.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.3)
                fig2.update_layout(paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                                   font_color="white", margin=dict(t=30,b=10))
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Top 15 Gainers & 15 Losers  (Total Return from Issue Price)")
            top = pd.concat([
                priced.nlargest(15, "Total Gain %"),
                priced.nsmallest(15, "Total Gain %"),
            ]).drop_duplicates().sort_values("Total Gain %")

            fig3 = px.bar(
                top, x="Total Gain %", y="Company", orientation="h",
                color="Total Gain %",
                color_continuous_scale=["#ff1744","#b71c1c","#1b5e20","#00c853"],
                color_continuous_midpoint=0,
                hover_data=["Type","Listed","Issue ₹"],
                text="Total Gain %",
            )
            fig3.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig3.update_layout(
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                font_color="white", height=700,
                margin=dict(t=20,b=10), showlegend=False,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig3, use_container_width=True)

    # ── Tab 3: Company detail ─────────────────────────────────────────────────
    with tab_detail:
        companies = df["Company"].dropna().tolist()
        sel = st.selectbox("Select a company", companies)
        row = df[df["Company"] == sel]
        if not row.empty:
            r = row.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Issue Price",    fmt_price(r["Issue ₹"]))
            c2.metric("Listing Close",  fmt_price(r["List Close ₹"]))
            c3.metric("Live CMP",       fmt_price(r["CMP ₹"]))
            c4.metric("Total Gain",     fmt_pct(r["Total Gain %"]))

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Listing Gain",   fmt_pct(r["List Gain %"]))
            c6.metric("Day Change",     fmt_pct(r["Day Chg %"]))
            c7.metric("52W High",       fmt_price(r["52W High ₹"]))
            c8.metric("52W Low",        fmt_price(r["52W Low ₹"]))

            if r.get("Business"):
                st.markdown(f"**Business:** {r['Business']}")
            if r.get("Symbol"):
                st.markdown(f"**NSE Symbol:** `{r['Symbol']}`")
                st.markdown(
                    f"[View on NSE](https://www.nseindia.com/get-quotes/equity?symbol={r['Symbol']}) | "
                    f"[View on Moneycontrol](https://www.moneycontrol.com/stocks/cptmarket/compsearchnew.php?search_data={r['Company'].replace(' ','+')})"
                )

    # ── Auto-refresh (meta tag) ───────────────────────────────────────────────
    st.markdown(
        f'<meta http-equiv="refresh" content="{REFRESH_SECS}">',
        unsafe_allow_html=True,
    )
    st.caption(f"⏱ Next auto-refresh in ~5 min")


if __name__ == "__main__":
    main()
