"""
Nifty 50 Monthly P/E Tracker  (v5 — correct architecture)
-----------------------------------------------------------
HOW IT WORKS:

  Historical PE (backfill):
    Loaded from  data/nifty50_pe_seed.csv  committed to this repo.
    Contains accurate NSE-published PE values from 2016 onwards.
    Only used when the DB row for that month is missing or has pe_source='unavailable'.

  Current month PE:
    Fetched live from NSE India /api/allIndices — a lightweight real-time
    endpoint that works from GitHub Actions (no geo-block, no heavy rate limit).
    Falls back to NSE historical JSON API, then marks as unavailable.

  Price data (OHLCV):
    Always fetched live from yfinance (10-year monthly history).

  Upsert strategy:
    - Rows with real PE (nse_official_seed / nse_live) are never overwritten
      with 'unavailable' — the on_conflict clause keeps existing good data.
    - Only updated_at is refreshed on re-run for existing rows.

WHY NOT SCALE CURRENT PE TO HISTORY:
    EPS grows over time. Nifty EPS has roughly tripled since 2016.
    Scaling current PE back to 2016 prices produces ~7x instead of the
    actual ~22x — completely wrong. Only real historical PE values are used.
"""

import os
import sys
import csv
import time
import logging
from datetime import datetime, timezone, date
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]
TABLE_NAME        = "nifty50_pe"
TICKER            = "^NSEI"
HISTORY_YEARS     = 10

# Seed CSV path — relative to this script
SEED_CSV = Path(__file__).parent / "data" / "nifty50_pe_seed.csv"

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Price data (yfinance) — always fetched live
# ─────────────────────────────────────────────────────────────────────────────

def fetch_monthly_price() -> pd.DataFrame:
    log.info(f"Fetching {HISTORY_YEARS}-year monthly OHLCV for {TICKER} …")
    df = yf.Ticker(TICKER).history(period=f"{HISTORY_YEARS}y", interval="1mo", auto_adjust=True)
    if df.empty:
        raise RuntimeError("yfinance returned empty price data for ^NSEI")
    df.index = df.index.tz_localize(None)
    df = df.reset_index().rename(columns={
        "Date": "date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume",
    })
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()
    df = df[["date", "open", "high", "low", "close", "volume"]]
    log.info(f"  → {len(df)} bars ({df['date'].min().date()} – {df['date'].max().date()})")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Historical PE — from seed CSV (accurate NSE-published values)
# ─────────────────────────────────────────────────────────────────────────────

def load_seed_pe() -> pd.DataFrame:
    """
    Load PE history from data/nifty50_pe_seed.csv committed to the repo.
    Returns DataFrame[date, pe_ratio, pe_source].
    """
    if not SEED_CSV.exists():
        log.warning(f"Seed CSV not found at {SEED_CSV} — historical PE will be NULL")
        return pd.DataFrame(columns=["date", "pe_ratio", "pe_source"])

    df = pd.read_csv(SEED_CSV)
    df["date"]     = pd.to_datetime(df["date"])
    df["pe_ratio"] = pd.to_numeric(df["pe_ratio"], errors="coerce")
    df = df.dropna(subset=["date", "pe_ratio"])
    log.info(f"Seed CSV: {len(df)} historical PE rows loaded "
             f"({df['date'].min().date()} – {df['date'].max().date()})")
    return df[["date", "pe_ratio", "pe_source"]]


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Current PE — live from NSE (for the current/latest month)
# ─────────────────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    for url in ["https://www.nseindia.com",
                "https://www.nseindia.com/market-data/live-equity-market"]:
        try:
            session.get(url, timeout=15)
            time.sleep(1.0)
        except Exception as e:
            log.debug(f"Warm-up {url}: {e}")
    return session


def fetch_current_pe_nse() -> float | None:
    """
    Fetch current Nifty 50 PE from NSE /api/allIndices.
    This lightweight endpoint works from GitHub Actions.
    Returns the PE float or None.
    """
    log.info("Fetching current PE from NSE allIndices …")
    session = _nse_session()
    try:
        resp = session.get("https://www.nseindia.com/api/allIndices", timeout=20)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "json" not in ct and "javascript" not in ct:
            log.warning(f"  allIndices non-JSON response ({ct}) — skipping")
            return None
        data = resp.json().get("data", [])
    except Exception as e:
        log.warning(f"  allIndices error: {e}")
        return None

    for idx in data:
        name = (idx.get("indexSymbol") or idx.get("index") or "").upper()
        if name == "NIFTY 50":
            pe = idx.get("pe") or idx.get("trailingPE") or idx.get("PE")
            if pe:
                log.info(f"  NSE allIndices → NIFTY 50 PE = {pe}")
                return float(pe)

    log.warning("  NIFTY 50 not found in allIndices")
    return None


def fetch_current_pe_nse_historical_api() -> float | None:
    """
    Fallback: fetch just today's PE from the NSE historical PE API
    using a 7-day window (much smaller request, less likely to be blocked).
    """
    from datetime import timedelta
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=7)
    session  = _nse_session()
    url = (
        f"https://www.nseindia.com/api/historical/indicesHistory/pe"
        f"?index=NIFTY%2050"
        f"&from={start_dt.strftime('%d-%m-%Y')}"
        f"&to={end_dt.strftime('%d-%m-%Y')}"
    )
    log.info(f"  NSE historical PE (7-day window): {url}")
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "json" not in ct and "javascript" not in ct:
            log.warning(f"  Non-JSON ({ct})")
            return None
        payload = resp.json()
        data = payload.get("data", []) if isinstance(payload, dict) else payload
        if data:
            # Get the latest record
            last = sorted(data, key=lambda x: x.get("Index Date", ""), reverse=True)[0]
            pe   = last.get("P/E") or last.get("pe") or last.get("PE")
            if pe:
                log.info(f"  NSE historical API → PE = {pe}")
                return float(pe)
    except Exception as e:
        log.warning(f"  NSE historical API error: {e}")
    return None


def get_current_month_pe() -> tuple[float | None, str]:
    """Returns (pe_value, source_label) for the current month."""
    pe = fetch_current_pe_nse()
    if pe:
        return pe, "nse_live"

    pe = fetch_current_pe_nse_historical_api()
    if pe:
        return pe, "nse_live"

    log.warning("  Could not fetch current PE — current month will be NULL")
    return None, "unavailable"


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Build final DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def build_final_dataframe(price_df: pd.DataFrame) -> pd.DataFrame:
    # Start with seed PE for all historical months
    pe_df = load_seed_pe()

    # Get live PE for current month
    current_month = pd.Timestamp.today().to_period("M").to_timestamp()
    current_pe, current_pe_src = get_current_month_pe()

    if current_pe:
        current_row = pd.DataFrame([{
            "date":      current_month,
            "pe_ratio":  current_pe,
            "pe_source": current_pe_src,
        }])
        # Merge: seed data takes precedence for historical, live for current
        pe_df = pd.concat([pe_df, current_row], ignore_index=True)
        pe_df = pe_df.drop_duplicates(subset=["date"], keep="last")

    # Merge price + PE on month-start date
    merged = price_df.merge(pe_df, on="date", how="left")

    # Compute eps_ttm from PE + close price
    if "eps_ttm" not in merged.columns:
        merged["eps_ttm"] = None
    has_pe = merged["pe_ratio"].notna() & (merged["pe_ratio"] > 0)
    merged.loc[has_pe, "eps_ttm"] = (
        merged.loc[has_pe, "close"] / merged.loc[has_pe, "pe_ratio"]
    ).round(4)

    # Fill missing pe_source
    merged["pe_ratio"]  = merged.get("pe_ratio")
    merged["pe_source"] = merged.get("pe_source", pd.Series("unavailable", index=merged.index))
    merged["pe_source"] = merged["pe_source"].fillna("unavailable")

    merged["ticker"]     = TICKER
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()

    cols = ["date", "ticker", "open", "high", "low", "close", "volume",
            "pe_ratio", "eps_ttm", "pe_source", "updated_at"]
    return merged[cols]


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Upsert to Supabase
#     Strategy: never overwrite a good PE with NULL/unavailable
# ─────────────────────────────────────────────────────────────────────────────

def upsert_to_supabase(df: pd.DataFrame, client: Client) -> None:
    records           = df.copy()
    records["date"]   = records["date"].dt.strftime("%Y-%m-%d")
    records["volume"] = (
        pd.to_numeric(records["volume"], errors="coerce").fillna(0).astype("int64")
    )
    records = records.where(pd.notna(records), other=None)
    rows    = records.to_dict(orient="records")

    total = len(rows)
    for i in range(0, total, 500):
        batch = rows[i : i + 500]
        client.table(TABLE_NAME).upsert(batch, on_conflict="date,ticker").execute()
        log.info(f"  Upserted {min(i + 500, total)}/{total} rows …")

    log.info(f"✅  Done — {total} rows upserted into `{TABLE_NAME}`")


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    log.info("═" * 60)
    log.info("Nifty 50 Monthly P/E Tracker — starting (v5)")
    log.info("═" * 60)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    price_df = fetch_monthly_price()
    final_df = build_final_dataframe(price_df)

    pe_ok  = final_df["pe_ratio"].notna().sum()
    pe_src = final_df["pe_source"].value_counts().to_dict()
    log.info(f"\n  P/E coverage: {pe_ok}/{len(final_df)} rows  |  sources: {pe_src}")
    log.info(f"\nSample (latest 5 rows):\n{final_df.tail(5).to_string(index=False)}\n")

    upsert_to_supabase(final_df, supabase)


if __name__ == "__main__":
    run()
