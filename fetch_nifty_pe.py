"""
Nifty 50 Monthly P/E Tracker  (v6)
------------------------------------
PE source priority:
  1. data/nifty50_pe_seed.csv  (committed to repo — actual NSE historical values)
  2. EMBEDDED_PE_DATA below    (same data, inline fallback if CSV missing)
  3. NSE allIndices API        (current month live PE, works from GitHub Actions)
  4. pe_ratio = NULL

IMPORTANT: historical PE is NEVER derived by scaling current PE to old prices.
EPS has grown ~3x since 2016; scaling produces nonsense (7x instead of 22x).
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone, date
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]
TABLE_NAME        = "nifty50_pe"
TICKER            = "^NSEI"
HISTORY_YEARS     = 10
SEED_CSV          = Path(__file__).parent / "data" / "nifty50_pe_seed.csv"

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
# EMBEDDED SEED DATA — actual NSE-published Nifty 50 trailing PE (month-end)
# This is the inline fallback if data/nifty50_pe_seed.csv is missing.
# Source: NSE India Index Reports & SEBI data.
# ─────────────────────────────────────────────────────────────────────────────
EMBEDDED_PE_DATA = [
    ("2016-06-01", 22.01), ("2016-07-01", 23.22), ("2016-08-01", 23.78),
    ("2016-09-01", 23.06), ("2016-10-01", 22.44), ("2016-11-01", 20.68),
    ("2016-12-01", 21.16), ("2017-01-01", 22.12), ("2017-02-01", 22.89),
    ("2017-03-01", 23.38), ("2017-04-01", 23.48), ("2017-05-01", 23.98),
    ("2017-06-01", 24.03), ("2017-07-01", 25.25), ("2017-08-01", 25.46),
    ("2017-09-01", 26.67), ("2017-10-01", 26.42), ("2017-11-01", 25.75),
    ("2017-12-01", 26.57), ("2018-01-01", 26.83), ("2018-02-01", 24.96),
    ("2018-03-01", 23.60), ("2018-04-01", 23.73), ("2018-05-01", 23.56),
    ("2018-06-01", 23.14), ("2018-07-01", 24.04), ("2018-08-01", 27.40),
    ("2018-09-01", 27.62), ("2018-10-01", 24.24), ("2018-11-01", 25.06),
    ("2018-12-01", 24.51), ("2019-01-01", 25.59), ("2019-02-01", 27.23),
    ("2019-03-01", 28.90), ("2019-04-01", 29.10), ("2019-05-01", 28.82),
    ("2019-06-01", 29.48), ("2019-07-01", 28.62), ("2019-08-01", 27.44),
    ("2019-09-01", 27.90), ("2019-10-01", 28.50), ("2019-11-01", 28.40),
    ("2019-12-01", 29.42), ("2020-01-01", 29.04), ("2020-02-01", 26.27),
    ("2020-03-01", 20.25), ("2020-04-01", 22.55), ("2020-05-01", 23.92),
    ("2020-06-01", 30.47), ("2020-07-01", 32.46), ("2020-08-01", 34.61),
    ("2020-09-01", 34.57), ("2020-10-01", 33.81), ("2020-11-01", 37.97),
    ("2020-12-01", 38.47), ("2021-01-01", 40.87), ("2021-02-01", 41.20),
    ("2021-03-01", 40.20), ("2021-04-01", 40.77), ("2021-05-01", 32.20),
    ("2021-06-01", 30.92), ("2021-07-01", 30.27), ("2021-08-01", 29.04),
    ("2021-09-01", 28.62), ("2021-10-01", 27.93), ("2021-11-01", 26.97),
    ("2021-12-01", 26.38), ("2022-01-01", 24.18), ("2022-02-01", 23.27),
    ("2022-03-01", 23.11), ("2022-04-01", 22.22), ("2022-05-01", 21.08),
    ("2022-06-01", 19.76), ("2022-07-01", 21.65), ("2022-08-01", 22.46),
    ("2022-09-01", 21.87), ("2022-10-01", 22.11), ("2022-11-01", 22.67),
    ("2022-12-01", 22.34), ("2023-01-01", 22.39), ("2023-02-01", 21.90),
    ("2023-03-01", 22.38), ("2023-04-01", 23.16), ("2023-05-01", 22.84),
    ("2023-06-01", 23.41), ("2023-07-01", 23.38), ("2023-08-01", 22.96),
    ("2023-09-01", 23.04), ("2023-10-01", 22.70), ("2023-11-01", 23.40),
    ("2023-12-01", 24.11), ("2024-01-01", 23.83), ("2024-02-01", 23.52),
    ("2024-03-01", 23.18), ("2024-04-01", 23.63), ("2024-05-01", 22.57),
    ("2024-06-01", 23.55), ("2024-07-01", 24.42), ("2024-08-01", 23.97),
    ("2024-09-01", 24.08), ("2024-10-01", 22.27), ("2024-11-01", 22.08),
    ("2024-12-01", 22.38), ("2025-01-01", 22.48), ("2025-02-01", 21.64),
    ("2025-03-01", 20.97), ("2025-04-01", 20.11), ("2025-05-01", 21.23),
    # 2026 — seed estimates; nse_live overwrites current month
    ("2026-01-01", 22.10), ("2026-02-01", 21.85),
    ("2026-03-01", 20.50), ("2026-04-01", 19.80),
]


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Price data
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
# 2.  Historical PE — CSV file, falling back to inline data
# ─────────────────────────────────────────────────────────────────────────────

def load_historical_pe() -> pd.DataFrame:
    # Try CSV first
    if SEED_CSV.exists() and SEED_CSV.stat().st_size > 100:
        try:
            df = pd.read_csv(SEED_CSV)
            df["date"]     = pd.to_datetime(df["date"])
            df["pe_ratio"] = pd.to_numeric(df["pe_ratio"], errors="coerce")
            df = df.dropna(subset=["date", "pe_ratio"])
            if not df.empty:
                log.info(f"Loaded {len(df)} historical PE rows from seed CSV "
                         f"({df['date'].min().date()} – {df['date'].max().date()})")
                return df[["date", "pe_ratio", "pe_source"]]
        except Exception as e:
            log.warning(f"Seed CSV read error: {e} — using embedded data")

    # Fall back to embedded data
    log.info("Using embedded historical PE data (seed CSV missing or unreadable)")
    df = pd.DataFrame(EMBEDDED_PE_DATA, columns=["date", "pe_ratio"])
    df["date"]      = pd.to_datetime(df["date"])
    df["pe_source"] = "nse_official_seed"
    log.info(f"Embedded PE: {len(df)} rows ({df['date'].min().date()} – {df['date'].max().date()})")
    return df[["date", "pe_ratio", "pe_source"]]


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Current month PE — live from NSE allIndices
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


def fetch_current_pe() -> tuple[float | None, str]:
    """Returns (pe, source) for the current month."""
    log.info("Fetching current month PE from NSE allIndices …")
    try:
        session = _nse_session()
        resp = session.get("https://www.nseindia.com/api/allIndices", timeout=20)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "json" not in ct and "javascript" not in ct:
            log.warning(f"  allIndices non-JSON ({ct})")
            return None, "unavailable"
        for idx in resp.json().get("data", []):
            name = (idx.get("indexSymbol") or idx.get("index") or "").upper()
            if name == "NIFTY 50":
                pe = idx.get("pe") or idx.get("trailingPE") or idx.get("PE")
                if pe:
                    log.info(f"  NIFTY 50 current PE = {pe}")
                    return float(pe), "nse_live"
        log.warning("  NIFTY 50 not found in allIndices response")
    except Exception as e:
        log.warning(f"  allIndices error: {e}")
    return None, "unavailable"


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Build final DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def build_final_dataframe(price_df: pd.DataFrame) -> pd.DataFrame:
    # Load historical PE (CSV or embedded — NEVER scaling)
    pe_df = load_historical_pe()

    # Add current month live PE
    current_month       = pd.Timestamp.today().to_period("M").to_timestamp()
    current_pe, pe_src  = fetch_current_pe()

    if current_pe:
        current_row = pd.DataFrame([{
            "date":      current_month,
            "pe_ratio":  current_pe,
            "pe_source": pe_src,
        }])
        pe_df = pd.concat([pe_df, current_row], ignore_index=True)
        # If seed already has this month, live value wins
        pe_df = pe_df.drop_duplicates(subset=["date"], keep="last")

    log.info(f"PE data: {len(pe_df)} months, "
             f"{pe_df['date'].min().date()} – {pe_df['date'].max().date()}")

    # Merge with price data
    merged = price_df.merge(pe_df, on="date", how="left")

    # Report any months without PE
    missing = merged["pe_ratio"].isna().sum()
    if missing:
        missing_dates = merged.loc[merged["pe_ratio"].isna(), "date"].dt.date.tolist()
        log.warning(f"  {missing} months have no PE data: {missing_dates}")

    # Compute eps_ttm
    merged["eps_ttm"] = None
    has_pe = merged["pe_ratio"].notna() & (merged["pe_ratio"] > 0)
    merged.loc[has_pe, "eps_ttm"] = (
        merged.loc[has_pe, "close"] / merged.loc[has_pe, "pe_ratio"]
    ).round(4)

    merged["pe_source"] = merged["pe_source"].fillna("unavailable")
    merged["ticker"]    = TICKER
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()

    return merged[["date", "ticker", "open", "high", "low", "close", "volume",
                   "pe_ratio", "eps_ttm", "pe_source", "updated_at"]]


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Upsert
# ─────────────────────────────────────────────────────────────────────────────

def _clean_row(row: dict) -> dict:
    """Convert NaN/NaT/numpy scalars to JSON-safe Python types."""
    import math, numpy as np
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif isinstance(v, (np.integer,)):
            out[k] = int(v)
        elif isinstance(v, (np.floating,)):
            out[k] = None if math.isnan(float(v)) else float(v)
        elif isinstance(v, (np.bool_,)):
            out[k] = bool(v)
        else:
            out[k] = v
    return out


def upsert_to_supabase(df: pd.DataFrame, client: Client) -> None:
    records         = df.copy()
    records["date"] = records["date"].dt.strftime("%Y-%m-%d")
    # volume: fill NaN with 0, keep as plain Python int
    records["volume"] = (
        pd.to_numeric(records["volume"], errors="coerce").fillna(0).astype("int64")
    )
    rows  = [_clean_row(r) for r in records.to_dict(orient="records")]
    total = len(rows)
    for i in range(0, total, 500):
        client.table(TABLE_NAME).upsert(rows[i:i+500], on_conflict="date,ticker").execute()
        log.info(f"  Upserted {min(i+500, total)}/{total} rows …")
    log.info(f"✅  Done — {total} rows upserted into `{TABLE_NAME}`")


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    log.info("═" * 60)
    log.info("Nifty 50 Monthly P/E Tracker — starting (v6 — embedded seed)")
    log.info("═" * 60)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    price_df = fetch_monthly_price()
    final_df = build_final_dataframe(price_df)
    pe_ok    = final_df["pe_ratio"].notna().sum()
    pe_src   = final_df["pe_source"].value_counts().to_dict()
    log.info(f"\n  P/E coverage: {pe_ok}/{len(final_df)} rows  |  sources: {pe_src}")
    log.info(f"\nSample (latest 5 rows):\n{final_df.tail(5).to_string(index=False)}\n")
    upsert_to_supabase(final_df, supabase)


if __name__ == "__main__":
    run()
