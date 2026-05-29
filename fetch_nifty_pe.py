"""
Nifty 50 Monthly P/E Tracker
-----------------------------
- Fetches monthly Nifty 50 OHLCV via yfinance
- Fetches OFFICIAL P/E data from NSE India's index data API
- Merges both and upserts into Supabase table `nifty50_pe`
- Safe to re-run: upsert is idempotent (no duplicates)

P/E Source priority:
  1. NSE India official P/E (most accurate, actual published values)
  2. yfinance trailingPE fallback (current snapshot scaled to history)
  3. NULL / unavailable
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from io import StringIO

import requests
import yfinance as yf
import pandas as pd
from supabase import create_client, Client

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL:  str = os.environ["SUPABASE_URL"]
SUPABASE_KEY:  str = os.environ["SUPABASE_KEY"]   # service_role key
TABLE_NAME       = "nifty50_pe"
TICKER           = "^NSEI"
HISTORY_YEARS    = 10

# NSE India headers — required to avoid 403
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Price data (yfinance)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_monthly_price() -> pd.DataFrame:
    """Download 10-year monthly OHLCV for Nifty 50 via yfinance."""
    log.info(f"Fetching {HISTORY_YEARS}-year monthly price data for {TICKER} …")
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period=f"{HISTORY_YEARS}y", interval="1mo", auto_adjust=True)

    if df.empty:
        raise RuntimeError("yfinance returned empty price data for ^NSEI")

    df.index = df.index.tz_localize(None)
    df = df.reset_index().rename(columns={
        "Date":   "date",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "close",
        "Volume": "volume",
    })
    # Normalise to month-start (YYYY-MM-01)
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()
    df = df[["date", "open", "high", "low", "close", "volume"]]
    log.info(f"  → {len(df)} monthly bars  ({df['date'].min().date()} – {df['date'].max().date()})")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2.  P/E data from NSE India (official source)
# ─────────────────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    """
    Create a requests Session with NSE cookies.
    NSE requires a valid cookie (obtained by hitting the homepage first).
    """
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        # Seed the session cookie
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)
    except Exception as e:
        log.warning(f"NSE cookie seed failed: {e}")
    return session


def fetch_nse_pe_history(years: int = 10) -> pd.DataFrame:
    """
    Fetch historical daily P/E, P/B, Dividend Yield for NIFTY 50 from NSE India.

    NSE provides a CSV download at:
      https://www.nseindia.com/api/historical/indicesHistory/pe?
            index=NIFTY%2050&from=<DD-MM-YYYY>&to=<DD-MM-YYYY>

    The API returns JSON with a `data` array.  Each element has:
      {
        "Index Name": "Nifty 50",
        "Index Date": "01-01-2015",
        "P/E":        "22.09",
        "P/B":        "3.33",
        "Div Yield":  "1.40"
      }

    Returns DataFrame[date, pe_ratio, pe_source]  indexed at month-start.
    """
    from datetime import date
    from dateutil.relativedelta import relativedelta

    end_date   = date.today()
    start_date = end_date - relativedelta(years=years)

    url = (
        "https://www.nseindia.com/api/historical/indicesHistory/pe"
        f"?index=NIFTY%2050"
        f"&from={start_date.strftime('%d-%m-%Y')}"
        f"&to={end_date.strftime('%d-%m-%Y')}"
    )

    log.info(f"Fetching NSE P/E history  ({start_date} → {end_date}) …")
    session = _nse_session()

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        log.warning(f"NSE API call failed: {e}")
        return pd.DataFrame()

    # The response structure varies slightly; handle both shapes
    data = []
    if isinstance(payload, dict):
        data = payload.get("data", payload.get("indexCloseOnlineRecords", []))
    elif isinstance(payload, list):
        data = payload

    if not data:
        log.warning("NSE API returned empty data array")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} daily P/E records from NSE")

    # Normalise column names (NSE can return different casings)
    df.columns = [c.strip() for c in df.columns]
    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if "date" in cl:
            col_map[c] = "date"
        elif cl in ("p/e", "pe", "pe_ratio"):
            col_map[c] = "pe_ratio"
    df = df.rename(columns=col_map)

    if "date" not in df.columns or "pe_ratio" not in df.columns:
        log.warning(f"Unexpected NSE response columns: {list(df.columns)}")
        return pd.DataFrame()

    df["date"]     = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["pe_ratio"] = pd.to_numeric(df["pe_ratio"], errors="coerce")
    df = df.dropna(subset=["date", "pe_ratio"])

    # Collapse to month-start: take the LAST trading day's PE in each month
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly = (
        df.sort_values("date")
          .groupby("month", as_index=False)
          .last()[["month", "pe_ratio"]]
          .rename(columns={"month": "date"})
    )
    monthly["pe_source"] = "nse_official"
    log.info(f"  → {len(monthly)} monthly P/E values after aggregation")
    return monthly


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Fallback: yfinance trailingPE scaled to history
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yfinance_pe_fallback(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    If NSE data is unavailable, derive P/E from current yfinance trailingPE.
    PE_historical = historical_close / (current_price / current_trailingPE)
    This is an approximation — EPS is assumed constant at current level.
    """
    log.info("Using yfinance trailingPE fallback …")
    ticker_obj = yf.Ticker(TICKER)
    info       = ticker_obj.info

    trailing_pe   = info.get("trailingPE")
    current_price = info.get("regularMarketPrice") or info.get("currentPrice")

    if trailing_pe and current_price:
        current_eps = current_price / trailing_pe
        log.info(f"  Implied EPS = {current_eps:.4f}  (price={current_price}, PE={trailing_pe})")
        df = price_df[["date"]].copy()
        df["pe_ratio"]  = (price_df["close"] / current_eps).round(2)
        df["eps_ttm"]   = round(current_eps, 4)
        df["pe_source"] = "yfinance_scaled"
        return df
    else:
        log.warning("  trailingPE not in ticker.info — PE will be NULL")
        df = price_df[["date"]].copy()
        df["pe_ratio"]  = None
        df["eps_ttm"]   = None
        df["pe_source"] = "unavailable"
        return df


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Merge price + PE
# ─────────────────────────────────────────────────────────────────────────────

def build_final_dataframe(price_df: pd.DataFrame) -> pd.DataFrame:
    """Merge price data with the best available P/E series."""

    # --- Try NSE official P/E first ---
    pe_df = fetch_nse_pe_history(years=HISTORY_YEARS)

    if pe_df.empty:
        log.warning("NSE P/E fetch failed — falling back to yfinance scaling")
        pe_df = fetch_yfinance_pe_fallback(price_df)

    # Merge on month-start date
    merged = price_df.merge(pe_df, on="date", how="left")

    # Fill any months where NSE had no data (e.g. current partial month)
    # with yfinance-scaled values
    missing_pe = merged["pe_ratio"].isna()
    if missing_pe.any():
        log.info(f"  {missing_pe.sum()} months still missing P/E — filling via yfinance fallback")
        fallback = fetch_yfinance_pe_fallback(price_df)
        fallback = fallback.set_index("date")
        for idx, row in merged[missing_pe].iterrows():
            fb = fallback.get(row["date"])
            if fb is not None:
                merged.at[idx, "pe_ratio"]  = fallback.loc[row["date"], "pe_ratio"]  if row["date"] in fallback.index else None
                merged.at[idx, "pe_source"] = fallback.loc[row["date"], "pe_source"] if row["date"] in fallback.index else "unavailable"

    # eps_ttm: for NSE-official rows compute implied EPS = close / PE
    if "eps_ttm" not in merged.columns:
        merged["eps_ttm"] = None
    mask_nse = merged["pe_source"] == "nse_official"
    merged.loc[mask_nse, "eps_ttm"] = (
        merged.loc[mask_nse, "close"] / merged.loc[mask_nse, "pe_ratio"]
    ).round(4)

    merged["ticker"]     = TICKER
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()

    cols = ["date", "ticker", "open", "high", "low", "close", "volume",
            "pe_ratio", "eps_ttm", "pe_source", "updated_at"]
    return merged[cols]


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Upsert to Supabase
# ─────────────────────────────────────────────────────────────────────────────

def upsert_to_supabase(df: pd.DataFrame, client: Client) -> None:
    records = df.copy()
    records["date"]   = records["date"].dt.strftime("%Y-%m-%d")
    records["volume"] = pd.to_numeric(records["volume"], errors="coerce").fillna(0).astype("int64")
    records = records.where(pd.notna(records), other=None)
    rows    = records.to_dict(orient="records")

    batch_size = 500
    total      = len(rows)
    inserted   = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        client.table(TABLE_NAME).upsert(batch, on_conflict="date,ticker").execute()
        inserted += len(batch)
        log.info(f"  Upserted {inserted}/{total} rows …")

    log.info(f"✅  Done — {total} rows upserted into `{TABLE_NAME}`")


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run():
    log.info("═" * 60)
    log.info("Nifty 50 Monthly P/E Tracker — starting")
    log.info("═" * 60)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    price_df = fetch_monthly_price()
    final_df = build_final_dataframe(price_df)

    # Summary stats
    pe_ok  = final_df["pe_ratio"].notna().sum()
    pe_src = final_df["pe_source"].value_counts().to_dict()
    log.info(f"\n  P/E coverage: {pe_ok}/{len(final_df)} rows  |  sources: {pe_src}")
    log.info(f"\nSample (latest 5 rows):\n{final_df.tail(5).to_string(index=False)}\n")

    upsert_to_supabase(final_df, supabase)


if __name__ == "__main__":
    run()
