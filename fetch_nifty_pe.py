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
HISTORY_YEARS     = 20
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
    # Source: NSE India official data + Craytheon monthly averages (sourced from NSE)
    # Note: Apr 2021 NSE switched standalone → consolidated earnings (PE dropped ~40→32)
    ("1999-01-01", 11.62), ("1999-02-01", 12.48), ("1999-03-01", 13.50), ("1999-04-01", 16.31),
    ("1999-05-01", 14.95), ("1999-06-01", 18.25), ("1999-07-01", 17.84),
    ("1999-08-01", 19.63), ("1999-09-01", 21.38), ("1999-10-01", 21.81),
    ("1999-11-01", 19.78), ("1999-12-01", 21.69), ("2000-01-01", 25.91),
    ("2000-02-01", 25.13), ("2000-03-01", 27.35), ("2000-04-01", 24.70),
    ("2000-05-01", 20.00), ("2000-06-01", 22.46), ("2000-07-01", 23.31),
    ("2000-08-01", 20.58), ("2000-09-01", 21.69), ("2000-10-01", 19.48),
    ("2000-11-01", 18.16), ("2000-12-01", 19.34), ("2001-01-01", 19.06),
    ("2001-02-01", 22.02), ("2001-03-01", 20.34), ("2001-04-01", 17.05),
    ("2001-05-01", 14.63), ("2001-06-01", 15.47), ("2001-07-01", 15.65),
    ("2001-08-01", 15.01), ("2001-09-01", 15.02), ("2001-10-01", 13.10),
    ("2001-11-01", 14.11), ("2001-12-01", 15.44), ("2002-01-01", 15.29),
    ("2002-02-01", 17.42), ("2002-03-01", 18.96), ("2002-04-01", 18.25),
    ("2002-05-01", 18.08), ("2002-06-01", 16.39), ("2002-07-01", 15.90),
    ("2002-08-01", 14.24), ("2002-09-01", 15.08), ("2002-10-01", 14.21),
    ("2002-11-01", 14.47), ("2002-12-01", 14.48), ("2003-01-01", 14.92),
    ("2003-02-01", 14.31), ("2003-03-01", 14.36), ("2003-04-01", 13.44),
    ("2003-05-01", 10.86), ("2003-06-01", 11.59), ("2003-07-01", 12.32),
    ("2003-08-01", 12.95), ("2003-09-01", 15.16), ("2003-10-01", 15.66),
    ("2003-11-01", 17.65), ("2003-12-01", 18.27), ("2004-01-01", 21.09),
    ("2004-02-01", 19.55), ("2004-03-01", 21.57), ("2004-04-01", 21.27),
    ("2004-05-01", 16.62), ("2004-06-01", 12.14), ("2004-07-01", 12.90),
    ("2004-08-01", 13.69), ("2004-09-01", 13.67), ("2004-10-01", 14.84),
    ("2004-11-01", 15.03), ("2004-12-01", 16.42), ("2005-01-01", 15.57),
    ("2005-02-01", 14.34), ("2005-03-01", 14.88), ("2005-04-01", 14.84),
    ("2005-05-01", 13.32), ("2005-06-01", 13.93), ("2005-07-01", 14.26),
    ("2005-08-01", 14.36), ("2005-09-01", 14.92), ("2005-10-01", 16.33),
    ("2005-11-01", 14.32), ("2005-12-01", 16.23), ("2006-01-01", 17.16),
    ("2006-02-01", 17.73), ("2006-03-01", 18.56), ("2006-04-01", 20.68),
    ("2006-05-01", 20.46), ("2006-06-01", 16.78), ("2006-07-01", 18.57),
    ("2006-08-01", 17.67), ("2006-09-01", 19.68), ("2006-10-01", 20.82),
    ("2006-11-01", 20.16), ("2006-12-01", 21.41), ("2007-01-01", 21.48),
    ("2007-02-01", 19.95), ("2007-03-01", 18.33), ("2007-04-01", 17.49),
    ("2007-05-01", 19.76), ("2007-06-01", 20.41), ("2007-07-01", 20.62),
    ("2007-08-01", 19.65), ("2007-09-01", 20.25), ("2007-10-01", 22.79),
    ("2007-11-01", 25.65), ("2007-12-01", 25.66), ("2008-01-01", 27.64),
    ("2008-02-01", 22.68), ("2008-03-01", 21.12), ("2008-04-01", 20.66),
    ("2008-05-01", 22.42), ("2008-06-01", 20.17), ("2008-07-01", 16.66),
    ("2008-08-01", 18.56), ("2008-09-01", 18.38), ("2008-10-01", 16.98),
    ("2008-11-01", 13.33), ("2008-12-01", 11.76), ("2009-01-01", 13.30),
    ("2009-02-01", 13.12), ("2009-03-01", 12.70), ("2009-04-01", 14.49),
    ("2009-05-01", 17.37), ("2009-06-01", 20.62), ("2009-07-01", 20.20),
    ("2009-08-01", 21.09), ("2009-09-01", 20.78), ("2009-10-01", 22.89),
    ("2009-11-01", 19.81), ("2009-12-01", 22.77), ("2010-01-01", 23.31),
    ("2010-02-01", 21.07), ("2010-03-01", 21.33), ("2010-04-01", 22.52),
    ("2010-05-01", 22.06), ("2010-06-01", 20.81), ("2010-07-01", 21.99),
    ("2010-08-01", 22.91), ("2010-09-01", 23.02), ("2010-10-01", 25.54),
    ("2010-11-01", 25.12), ("2010-12-01", 23.78), ("2011-01-01", 24.57),
    ("2011-02-01", 20.70), ("2011-03-01", 21.14), ("2011-04-01", 22.11),
    ("2011-05-01", 21.19), ("2011-06-01", 20.65), ("2011-07-01", 20.75),
    ("2011-08-01", 19.81), ("2011-09-01", 18.19), ("2011-10-01", 17.51),
    ("2011-11-01", 19.04), ("2011-12-01", 17.87), ("2012-01-01", 16.79),
    ("2012-02-01", 18.66), ("2012-03-01", 18.92), ("2012-04-01", 18.79),
    ("2012-05-01", 17.99), ("2012-06-01", 16.36), ("2012-07-01", 17.51),
    ("2012-08-01", 17.13), ("2012-09-01", 17.63), ("2012-10-01", 19.22),
    ("2012-11-01", 18.46), ("2012-12-01", 18.56), ("2013-01-01", 18.82),
    ("2013-02-01", 18.42), ("2013-03-01", 17.74), ("2013-04-01", 17.51),
    ("2013-05-01", 18.05), ("2013-06-01", 17.81), ("2013-07-01", 17.96),
    ("2013-08-01", 17.03), ("2013-09-01", 16.00), ("2013-10-01", 16.95),
    ("2013-11-01", 18.20), ("2013-12-01", 18.51), ("2014-01-01", 18.69),
    ("2014-02-01", 17.34), ("2014-03-01", 17.52), ("2014-04-01", 18.91),
    ("2014-05-01", 18.72), ("2014-06-01", 20.27), ("2014-07-01", 20.71),
    ("2014-08-01", 20.05), ("2014-09-01", 20.99), ("2014-10-01", 20.77),
    ("2014-11-01", 21.58), ("2014-12-01", 21.85), ("2015-01-01", 21.16),
    ("2015-02-01", 22.51), ("2015-03-01", 23.95), ("2015-04-01", 22.95),
    ("2015-05-01", 22.47), ("2015-06-01", 23.25), ("2015-07-01", 23.43),
    ("2015-08-01", 23.54), ("2015-09-01", 21.57), ("2015-10-01", 22.21),
    ("2015-11-01", 21.98), ("2015-12-01", 21.51), ("2016-01-01", 21.53),
    ("2016-02-01", 20.22), ("2016-03-01", 19.53), ("2016-04-01", 21.19),
    ("2016-05-01", 21.25),
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
    # 2025-06 onward: Craytheon monthly averages (NSE source)
    ("2025-06-01", 22.50), ("2025-07-01", 22.50), ("2025-08-01", 21.80),
    ("2025-09-01", 21.90), ("2025-10-01", 22.50), ("2025-11-01", 22.60),
    ("2025-12-01", 22.60), ("2026-01-01", 22.30), ("2026-02-01", 22.40),
    ("2026-03-01", 20.60), ("2026-04-01", 20.90),
    # Current month is always overwritten by nse_live fetch
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
