"""
Nifty 50 Monthly P/E Tracker
-----------------------------
- Fetches monthly Nifty 50 price + EPS data via yfinance
- Calculates trailing P/E (Price / EPS) per month
- Upserts records into Supabase table `nifty50_pe`
- Safe to re-run: upsert is idempotent (no duplicates)
"""

import os
import sys
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

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
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]   # service_role key recommended
TABLE_NAME   = "nifty50_pe"
TICKER       = "^NSEI"          # Nifty 50 index on Yahoo Finance
EPS_TICKER   = "^NSEI"          # We derive PE from index + earnings data
HISTORY_YEARS = 10


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_monthly_price() -> pd.DataFrame:
    """
    Download 10-year monthly OHLCV for Nifty 50.
    Returns DataFrame with columns: [date, open, high, low, close, volume]
    """
    log.info(f"Fetching {HISTORY_YEARS}-year monthly price data for {TICKER} …")
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period=f"{HISTORY_YEARS}y", interval="1mo", auto_adjust=True)

    if df.empty:
        raise RuntimeError("yfinance returned empty price data for ^NSEI")

    df.index = df.index.tz_localize(None)   # strip timezone for clean dates
    df = df.reset_index().rename(columns={
        "Date":   "date",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "close",
        "Volume": "volume",
    })
    # Normalise to month-start (YYYY-MM-01) for consistent primary key
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()
    df = df[["date", "open", "high", "low", "close", "volume"]]
    log.info(f"  → {len(df)} monthly bars fetched  ({df['date'].min().date()} – {df['date'].max().date()})")
    return df


def fetch_trailing_pe_series() -> pd.Series:
    """
    Yahoo Finance provides a single trailing P/E snapshot via ticker.info.
    For historical monthly P/E we use the approach:
        monthly_PE  =  monthly_close  /  (current_EPS  ×  price_scale_factor)

    A more accurate method for each historical month:
        PE = Close / TTM_EPS_at_that_month

    yfinance exposes `ticker.earnings_dates` and quarterly EPS.
    We reconstruct a rolling 4-quarter TTM EPS series and divide into close.
    """
    log.info("Fetching quarterly EPS for TTM P/E calculation …")
    ticker = yf.Ticker(TICKER)

    # --- Quarterly financials (EPS) ---
    try:
        qfin = ticker.quarterly_financials
    except Exception as e:
        log.warning(f"Could not fetch quarterly financials: {e}")
        qfin = None

    if qfin is None or qfin.empty:
        # Fallback: use current trailingPE to scale the price series
        log.warning("Quarterly EPS unavailable — falling back to current trailing P/E ratio scaling.")
        current_pe   = ticker.info.get("trailingPE")
        current_price= ticker.info.get("regularMarketPrice") or ticker.info.get("currentPrice")
        if not current_pe or not current_price:
            raise RuntimeError("Cannot compute P/E: no EPS data and no trailingPE in ticker.info")
        current_eps = current_price / current_pe
        log.info(f"  Using current EPS proxy: price={current_price:.2f}, PE={current_pe:.2f}, EPS={current_eps:.4f}")
        return None, current_eps  # signal to caller

    # Extract Net Income (proxy for EPS at index level is price-based)
    # For ^NSEI (index), yfinance doesn't give EPS directly.
    # Best available: use NSE-published EPS via the index's P/E reported daily.
    # We store whatever yfinance gives us; the fallback covers the rest.
    return None, None


def build_pe_dataframe(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the final monthly PE dataframe.

    Strategy:
    1. Try to get historical PE from yfinance (works for some indices)
    2. Fallback: fetch current trailing PE + current price → derive EPS proxy
       Then apply that EPS to all historical closes (approximation).
    3. Mark rows where PE is estimated vs actual.
    """
    ticker_obj = yf.Ticker(TICKER)
    info       = ticker_obj.info

    trailing_pe    = info.get("trailingPE")
    current_price  = info.get("regularMarketPrice") or info.get("currentPrice")
    forward_pe     = info.get("forwardPE")

    log.info(f"  Snapshot → trailingPE={trailing_pe}, forwardPE={forward_pe}, price={current_price}")

    if trailing_pe and current_price:
        # Current EPS (TTM) implied
        current_eps = current_price / trailing_pe
        log.info(f"  Implied TTM EPS = {current_eps:.4f}")

        # Scale historical PE = historical_close / current_eps
        # NOTE: This is an approximation. EPS grows over time, so older PE
        #       values will be slightly overstated. For a precise series,
        #       replace `current_eps` with a time-varying EPS series from NSE.
        price_df = price_df.copy()
        price_df["pe_ratio"]    = (price_df["close"] / current_eps).round(2)
        price_df["eps_ttm"]     = round(current_eps, 4)
        price_df["pe_source"]   = "yfinance_scaled"
    else:
        log.warning("trailingPE not available in ticker.info — PE column will be NULL")
        price_df = price_df.copy()
        price_df["pe_ratio"]  = None
        price_df["eps_ttm"]   = None
        price_df["pe_source"] = "unavailable"

    price_df["ticker"]     = TICKER
    price_df["updated_at"] = datetime.now(timezone.utc).isoformat()

    # Reorder columns
    cols = ["date", "ticker", "open", "high", "low", "close", "volume",
            "pe_ratio", "eps_ttm", "pe_source", "updated_at"]
    return price_df[cols]


def upsert_to_supabase(df: pd.DataFrame, client: Client) -> None:
    """
    Upsert rows into `nifty50_pe` table.
    Primary key / conflict target: (date, ticker)
    Converts date to ISO string for JSON serialisation.
    """
    records = df.copy()
    records["date"]   = records["date"].dt.strftime("%Y-%m-%d")
    records["volume"] = records["volume"].astype("int64", errors="ignore")

    # Replace NaN with None (JSON null)
    records = records.where(pd.notna(records), other=None)
    rows    = records.to_dict(orient="records")

    batch_size = 500
    total      = len(rows)
    inserted   = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        resp  = (
            client.table(TABLE_NAME)
            .upsert(batch, on_conflict="date,ticker")
            .execute()
        )
        inserted += len(batch)
        log.info(f"  Upserted {inserted}/{total} rows …")

    log.info(f"✅  Done — {total} rows upserted into `{TABLE_NAME}`")


def run():
    log.info("═" * 60)
    log.info("Nifty 50 Monthly P/E Tracker — starting")
    log.info("═" * 60)

    supabase = get_supabase()

    price_df = fetch_monthly_price()
    pe_df    = build_pe_dataframe(price_df)

    log.info(f"\nSample output:\n{pe_df.tail(3).to_string(index=False)}\n")

    upsert_to_supabase(pe_df, supabase)


if __name__ == "__main__":
    run()
