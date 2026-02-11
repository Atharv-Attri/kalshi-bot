import time
from pathlib import Path
import os
os.environ["STREAMLIT_SUPPRESS_DEPRECATION_WARNINGS"] = "true"
import pandas as pd
import streamlit as st
import altair as alt
from types import SimpleNamespace
import warnings

series_ns = SimpleNamespace(**{
    "NBA": "KXNBAGAME",
    "NCAA_BB_M": "KXNCAAMBGAME",
    "NCAA_BB_W": "KXNCAAWBGAME",
})

SERIES_MAP = {
    series_ns.NBA: "NBA",
    series_ns.NCAA_BB_M: "NCAA MBB",
    series_ns.NCAA_BB_W: "NCAA WBB",
}

# Optional: quiet normal warnings
warnings.filterwarnings("ignore")

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Trading Bot Live Monitor",
    page_icon="📈",
    layout="wide",
)

# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.title("⚙️ Dashboard Settings")

log_file = st.sidebar.text_input("Log file", "../data/log.csv")
refresh_sec = st.sidebar.slider("Refresh every X seconds", 1, 30, 5)
recent_n = st.sidebar.slider("Show last N events in main table", 10, 200, 40)

st.sidebar.markdown("---")
sidebar_feed_placeholder = st.sidebar.empty()
main_placeholder = st.empty()  # main container we rerender each cycle


# -----------------------------
# Helpers
# -----------------------------
def load_log(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["ticker", "dir", "action", "price", "effect"])

    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]

    for col in ["ticker", "dir", "action", "price", "effect"]:
        if col not in df.columns:
            df[col] = None

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["effect"] = pd.to_numeric(df["effect"], errors="coerce").fillna(0.0)
    df["action"] = df["action"].astype(str).str.lower()
    df["dir"] = df["dir"].astype(str)

    df["event"] = range(1, len(df) + 1)
    return df


def compute_open_positions(df: pd.DataFrame) -> pd.DataFrame:
    # Track unmatched opens per ticker
    if df.empty:
        return pd.DataFrame(columns=["ticker", "direction", "avg_open_price"])

    stacks = {}
    for _, row in df.iterrows():
        t = row["ticker"]
        stacks.setdefault(t, [])

        if row["action"] == "open":
            stacks[t].append({"price": row["price"], "dir": row["dir"]})
        elif row["action"] == "close" and stacks[t]:
            stacks[t].pop()

    recs = []
    for t, stack in stacks.items():
        if not stack:
            continue
        direction = stack[-1]["dir"]
        prices = [p["price"] for p in stack if pd.notna(p["price"])]
        avg_price = sum(prices) / len(prices) if prices else None
        recs.append(
            {"ticker": t, "direction": direction.upper(), "avg_open_price": avg_price}
        )

    if not recs:
        return pd.DataFrame(columns=["ticker", "direction", "avg_open_price"])

    out = pd.DataFrame(recs).sort_values("ticker")
    return out[["ticker", "direction", "avg_open_price"]]


def compute_kpis(df: pd.DataFrame) -> tuple[float, float, float]:
    # Only closes count for win rate and realized effect
    closes = df[df["action"] == "close"]
    if closes.empty:
        return 0.0, 0.0, 0.0
    realized = closes["effect"].sum()
    win_rate = (closes["effect"] > 0).mean()
    avg_effect = closes["effect"].mean()
    return float(realized), float(win_rate), float(avg_effect)


def render_sidebar_feed(df: pd.DataFrame, max_items: int = 8) -> None:
    if df.empty:
        sidebar_feed_placeholder.info("Waiting for activity...")
        return

    notif_df = df[df["action"].isin(["open", "close"])].tail(max_items)
    if notif_df.empty:
        sidebar_feed_placeholder.info("No opens or closes yet.")
        return

    notif_df = notif_df.iloc[::-1]  # newest first

    css = """
<style>
.glass-feed-wrap {
    margin-top: 0.5rem;
}
.glass-pill {
    position: relative;
    border-radius: 0.9rem;
    padding: 0.55rem 0.7rem;
    margin-bottom: 0.5rem;
    background: linear-gradient(
        135deg,
        rgba(15,23,42,0.96),
        rgba(31,41,55,0.9)
    );
    border: 1px solid rgba(148,163,184,0.35);
    box-shadow:
        0 14px 30px rgba(15,23,42,0.8),
        inset 0 0 0 1px rgba(15,23,42,0.8);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    font-size: 0.75rem;
    color: #e5e7eb;
}
.glass-pill-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.15rem;
}
.glass-pill-ticker {
    font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 0.78rem;
    font-weight: 600;
    color: #f9fafb;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 9.5rem;
}
.glass-pill-tag-open,
.glass-pill-tag-close {
    font-size: 0.68rem;
    font-weight: 600;
    padding: 0.08rem 0.45rem;
    border-radius: 999px;
    border: 1px solid;
}
.glass-pill-tag-open {
    background: radial-gradient(circle at top right, rgba(34,197,94,0.18), rgba(6,95,70,0.5));
    color: #bbf7d0;
    border-color: rgba(34,197,94,0.7);
}
.glass-pill-tag-close {
    background: radial-gradient(circle at top right, rgba(248,113,113,0.22), rgba(127,29,29,0.6));
    color: #fee2e2;
    border-color: rgba(248,113,113,0.85);
}
.glass-pill-body {
    font-size: 0.72rem;
    color: #cbd5f5;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.glass-pill-left {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
}
.glass-pill-right {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
}
.glass-pill-series {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #9ca3af;
}
.glass-dir-plain {
    font-size: 0.72rem;
    font-weight: 600;
    color: #e5e7eb;
}
.glass-pill-label {
    opacity: 0.7;
}
.glass-pill-separator {
    opacity: 0.6;
}
</style>
"""

    def short_label(ticker: str) -> str:
        if not isinstance(ticker, str):
            return "unknown"
        parts = ticker.split("-")
        if len(parts) >= 3:
            tail = parts[-2]
            side = parts[-1]
            tail_clean = tail
            while tail_clean and tail_clean[0].isdigit():
                tail_clean = tail_clean[1:]
            if tail_clean:
                return f"{tail_clean} ({side})"
            return f"{tail} ({side})"
        if len(ticker) > 18:
            return ticker[:15] + "..."
        return ticker

    def series_label(ticker: str) -> str:
        if not isinstance(ticker, str):
            return ""
        prefix = ticker.split("-")[0]
        return SERIES_MAP.get(prefix, prefix)

    html = css + '<div class="glass-feed-wrap"><h4>Recent actions</h4>'

    for _, row in notif_df.iterrows():
        is_open = row["action"] == "open"
        tag_class = "glass-pill-tag-open" if is_open else "glass-pill-tag-close"
        tag_text = "OPEN" if is_open else "CLOSE"

        ticker_full = row["ticker"]
        ticker_display = short_label(ticker_full)
        series_str = series_label(ticker_full)

        dir_text = str(row["dir"]).upper()

        price_str = f"{row['price']:.3f}" if pd.notna(row["price"]) else "n/a"
        effect_str = f"{row['effect'] * 100:.2f}%"

        if is_open:
            right_label = "@"
            right_value = price_str
        else:
            right_label = "PnL"
            right_value = effect_str

        html += f"""
<div class="glass-pill" title="{ticker_full}">
  <div class="glass-pill-header">
    <div class="glass-pill-ticker">{ticker_display}</div>
    <div class="{tag_class}">{tag_text}</div>
  </div>
  <div class="glass-pill-body">
    <div class="glass-pill-left">
      <span class="glass-pill-series">{series_str}</span>
    </div>
    <div class="glass-pill-right">
      <span class="glass-dir-plain">{dir_text}</span>
      <span class="glass-pill-separator">•</span>
      <span class="glass-pill-label">{right_label}</span>
      <span>{right_value}</span>
    </div>
  </div>
</div>
"""

    html += "</div>"

    sidebar_feed_placeholder.markdown(html, unsafe_allow_html=True)

# -----------------------------
# Live updating loop
# -----------------------------
while True:
    df = load_log(log_file)

    with main_placeholder.container():
        st.title("📊 Trading Bot Live Dashboard")

        if df.empty:
            st.warning("No log entries yet. Waiting for bot to write to log.csv")
        else:
            open_positions = compute_open_positions(df)
            realized_effect, win_rate, avg_effect = compute_kpis(df)
            open_count = len(open_positions) if not open_positions.empty else 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Log Events", len(df))
            k2.metric("Open Positions", open_count)
            k3.metric("Realized PnL", f"${realized_effect:.2f}")
            k4.metric("Win Rate", f"{win_rate * 100:.1f} %", f"Avg {avg_effect*100:.2f}%")

            st.subheader("Open Positions")
            if open_positions.empty:
                st.info("No open positions right now.")
            else:
                rows = len(open_positions)
                height = min(80 + rows * 32, 350)
                st.dataframe(
                    open_positions.style.format({"avg_open_price": "{:.3f}"}),
                    width="stretch",
                    height=height,
                )

            st.subheader("Cumulative PnL Over Time")
            df["cum_effect"] = df["effect"].cumsum() 
            effect_chart = (
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x="event",
                    y="cum_effect",
                    tooltip=["event", "ticker", "action", "price", "effect", "cum_effect"],
                )
            )
            st.altair_chart(effect_chart, use_container_width=True)

            st.subheader("Recent Events")
            recent_df = df.sort_values("event", ascending=False).head(recent_n)
            st.dataframe(
                recent_df[
                    ["event", "ticker", "dir", "action", "price", "effect"]
                ].sort_values("event", ascending=False),
                width="stretch",
                height=320,
            )

        st.caption(f"Auto refreshing every {refresh_sec} seconds")

    render_sidebar_feed(df, max_items=8)
    time.sleep(refresh_sec)
