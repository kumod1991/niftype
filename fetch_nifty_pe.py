
"""
NIFTY 50 MONTHLY P/E TRACKER - V8

Purpose:
    Maintain monthly NIFTY 50 OHLCV, trailing P/E and derived EPS
    in Supabase.

Historical PE source priority:
    1. data/nifty50_pe_seed.csv
    2. Existing historical records in Supabase
    3. NSE allIndices API - current month only

Important:
    - Historical PE is never estimated or interpolated.
    - Supabase fills only months missing from the CSV.
    - Current-month PE is obtained from NSE.
    - No NULL or invalid PE values are inserted.
    - All completed months must pass coverage validation.
    - Current-month prices are fetched separately.
"""

import os
import sys
import math
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import yfinance as yf
import pandas as pd
import numpy as np

from supabase import create_client, Client


# ============================================================================
# 1. CONFIGURATION
# ============================================================================

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TABLE_NAME = "nifty50_pe"
TICKER = "^NSEI"

HISTORY_YEARS = 10

IST = ZoneInfo("Asia/Kolkata")

SEED_CSV = (
    Path(__file__).parent
    / "data"
    / "nifty50_pe_seed.csv"
)

NSE_BASE_URL = "https://www.nseindia.com"

NSE_API_URL = (
    "https://www.nseindia.com/api/allIndices"
)

BATCH_SIZE = 500


# ============================================================================
# 2. LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger(__name__)


# ============================================================================
# 3. NSE HEADERS
# ============================================================================

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


# ============================================================================
# 4. DATE HELPERS
# ============================================================================

def get_current_month() -> pd.Timestamp:

    now_ist = datetime.now(IST)

    return (
        pd.Timestamp(now_ist.date())
        .to_period("M")
        .to_timestamp()
    )


def normalize_month(series: pd.Series) -> pd.Series:

    return (
        pd.to_datetime(series, errors="coerce")
        .dt.to_period("M")
        .dt.to_timestamp()
    )


# ============================================================================
# 5. NSE SESSION
# ============================================================================

def _nse_session() -> requests.Session:

    session = requests.Session()

    session.headers.update(NSE_HEADERS)

    try:

        response = session.get(
            NSE_BASE_URL,
            timeout=8,
        )

        response.raise_for_status()

        log.info("NSE session initialized.")

    except requests.RequestException as e:

        log.warning(
            f"NSE warm-up failed: {e}"
        )

    return session


# ============================================================================
# 6. FETCH MONTHLY NIFTY PRICE DATA
# ============================================================================

def fetch_monthly_price() -> pd.DataFrame:

    log.info(
        f"Fetching {HISTORY_YEARS}-year monthly OHLCV "
        f"for {TICKER}..."
    )

    df = yf.Ticker(TICKER).history(
        period=f"{HISTORY_YEARS}y",
        interval="1mo",
        auto_adjust=True,
    )

    if df.empty:

        raise RuntimeError(
            "yfinance returned empty monthly price data."
        )

    if getattr(df.index, "tz", None) is not None:

        df.index = df.index.tz_localize(None)

    df = df.reset_index()

    date_column = (
        "Date" if "Date" in df.columns else "Datetime"
    )

    df = df.rename(
        columns={
            date_column: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    required_columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:

        raise RuntimeError(
            f"Missing yfinance columns: {missing_columns}"
        )

    df = df[required_columns].copy()

    df["date"] = normalize_month(df["date"])

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    df = df.dropna(
        subset=[
            "date",
            "open",
            "high",
            "low",
            "close",
        ]
    )

    df = (
        df.sort_values("date")
        .drop_duplicates("date", keep="last")
        .reset_index(drop=True)
    )

    if df.empty:

        raise RuntimeError(
            "No valid monthly price records."
        )

    log.info(
        f"Fetched {len(df)} monthly bars "
        f"({df['date'].min().date()} - "
        f"{df['date'].max().date()})"
    )

    return df


# ============================================================================
# 7. LOAD HISTORICAL PE FROM CSV
# ============================================================================

def load_historical_pe() -> pd.DataFrame:

    log.info("Loading historical PE seed CSV...")

    if not SEED_CSV.exists():

        raise FileNotFoundError(
            f"Historical PE seed CSV not found: {SEED_CSV}"
        )

    if SEED_CSV.stat().st_size == 0:

        raise RuntimeError(
            "Historical PE seed CSV is empty."
        )

    df = pd.read_csv(SEED_CSV)

    required = [
        "date",
        "pe_ratio",
    ]

    for col in required:

        if col not in df.columns:

            raise RuntimeError(
                f"Seed CSV missing required column: {col}"
            )

    df["date"] = normalize_month(df["date"])

    df["pe_ratio"] = pd.to_numeric(
        df["pe_ratio"],
        errors="coerce",
    )

    df = df.dropna(
        subset=[
            "date",
            "pe_ratio",
        ]
    )

    df = df[
        df["pe_ratio"] > 0
    ].copy()

    if "pe_source" not in df.columns:

        df["pe_source"] = "historical_seed"

    df["pe_source"] = (
        df["pe_source"]
        .fillna("historical_seed")
    )

    df = (
        df[
            [
                "date",
                "pe_ratio",
                "pe_source",
            ]
        ]
        .sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    if df.empty:

        raise RuntimeError(
            "No valid historical PE values in seed CSV."
        )

    log.info(
        f"Loaded {len(df)} PE rows from CSV "
        f"({df['date'].min().date()} - "
        f"{df['date'].max().date()})"
    )

    return df


# ============================================================================
# 8. RECOVER HISTORICAL PE FROM SUPABASE
# ============================================================================

def recover_historical_pe_from_supabase(
    client: Client,
    pe_df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:

    log.info(
        "Checking Supabase for missing historical PE..."
    )

    if end_date < start_date:

        return pe_df

    start_key = start_date.strftime("%Y-%m-%d")
    end_key = end_date.strftime("%Y-%m-%d")

    try:

        result = (
            client
            .table(TABLE_NAME)
            .select(
                "date,pe_ratio,eps_ttm,pe_source"
            )
            .eq("ticker", TICKER)
            .gte("date", start_key)
            .lte("date", end_key)
            .order("date")
            .limit(1000)
            .execute()
        )

        rows = result.data or []

        log.info(
            f"Supabase historical records found: {len(rows)}"
        )

        if not rows:

            return pe_df

        existing = pd.DataFrame(rows)

        existing["date"] = normalize_month(
            existing["date"]
        )

        existing["pe_ratio"] = pd.to_numeric(
            existing["pe_ratio"],
            errors="coerce",
        )

        existing = existing.dropna(
            subset=[
                "date",
                "pe_ratio",
            ]
        )

        existing = existing[
            existing["pe_ratio"] > 0
        ].copy()

        if "pe_source" not in existing.columns:

            existing["pe_source"] = "supabase_existing"

        existing["pe_source"] = (
            existing["pe_source"]
            .fillna("supabase_existing")
        )

        existing = existing[
            [
                "date",
                "pe_ratio",
                "pe_source",
            ]
        ]

        existing = (
            existing
            .sort_values("date")
            .drop_duplicates(
                subset=["date"],
                keep="last",
            )
        )

        # Seed CSV has higher priority than Supabase.

        seed_dates = set(pe_df["date"])

        missing = existing[
            ~existing["date"].isin(seed_dates)
        ].copy()

        if missing.empty:

            log.info(
                "No missing historical months recovered from Supabase."
            )

            return pe_df

        pe_df = pd.concat(
            [
                pe_df,
                missing,
            ],
            ignore_index=True,
        )

        recovered_months = (
            missing["date"]
            .dt.strftime("%Y-%m")
            .tolist()
        )

        log.info(
            f"Recovered {len(missing)} missing PE months from Supabase."
        )

        log.info(
            f"Recovered months: {recovered_months}"
        )

        return (
            pe_df
            .sort_values("date")
            .drop_duplicates(
                subset=["date"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"Supabase historical recovery failed: {e}"
        )

        raise RuntimeError(
            "Could not verify historical PE against Supabase. "
            "Stopping to prevent incomplete data insertion."
        ) from e


# ============================================================================
# 9. VALIDATE HISTORICAL PE COVERAGE
# ============================================================================

def validate_historical_pe_coverage(
    price_df: pd.DataFrame,
    pe_df: pd.DataFrame,
) -> None:

    if price_df.empty:

        raise RuntimeError(
            "Price dataframe is empty."
        )

    if pe_df.empty:

        raise RuntimeError(
            "Historical PE dataframe is empty."
        )

    current_month = get_current_month()

    validation_end = (
        current_month
        - pd.DateOffset(months=1)
    )

    earliest_price_month = (
        price_df["date"]
        .min()
        .to_period("M")
        .to_timestamp()
    )

    required_months = pd.period_range(
        start=earliest_price_month,
        end=validation_end,
        freq="M",
    )

    available_months = set(
        pe_df["date"]
        .dropna()
        .dt.to_period("M")
    )

    missing = [
        str(month)
        for month in required_months
        if month not in available_months
    ]

    log.info(
        f"Historical PE validation: "
        f"{earliest_price_month.date()} to "
        f"{validation_end.date()}"
    )

    if missing:

        raise RuntimeError(
            "\n"
            "============================================\n"
            "HISTORICAL PE COVERAGE ERROR\n"
            "============================================\n"
            f"Missing {len(missing)} month(s):\n"
            f"{', '.join(missing)}\n"
            "No NULL historical PE values will be inserted.\n"
            "Check seed CSV and Supabase history.\n"
            "============================================"
        )

    log.info(
        "Historical PE coverage validation PASSED."
    )


# ============================================================================
# 10. FETCH CURRENT-MONTH PRICE DATA
# ============================================================================

def fetch_current_month_price() -> pd.DataFrame:

    current_month = get_current_month()

    month_key = current_month.strftime("%Y-%m")

    log.info(
        f"Fetching current-month MTD prices: {month_key}"
    )

    # ------------------------------------------------------------------------
    # First source: yfinance daily data
    # ------------------------------------------------------------------------

    try:

        df = yf.Ticker(TICKER).history(
            period="10d",
            interval="1d",
            auto_adjust=True,
        )

        if not df.empty:

            if getattr(df.index, "tz", None) is not None:

                df.index = df.index.tz_localize(None)

            df = df.reset_index()

            date_column = (
                "Date" if "Date" in df.columns else "Datetime"
            )

            df = df.rename(
                columns={
                    date_column: "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
            )

            df["date"] = pd.to_datetime(
                df["date"]
            ).dt.normalize()

            df = df[
                df["date"].dt.to_period("M")
                == current_month.to_period("M")
            ].copy()

            if not df.empty:

                for col in [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ]:

                    df[col] = pd.to_numeric(
                        df[col],
                        errors="coerce",
                    )

                df = df.dropna(
                    subset=[
                        "open",
                        "high",
                        "low",
                        "close",
                    ]
                )

                if not df.empty:

                    row = pd.DataFrame([
                        {
                            "date": current_month,
                            "open": float(df.iloc[0]["open"]),
                            "high": float(df["high"].max()),
                            "low": float(df["low"].min()),
                            "close": float(df.iloc[-1]["close"]),
                            "volume": int(
                                df["volume"]
                                .fillna(0)
                                .sum()
                            ),
                        }
                    ])

                    log.info(
                        f"Current-month price from yfinance: "
                        f"{row.iloc[0]['close']:.2f}"
                    )

                    return row

    except Exception as e:

        log.warning(
            f"yfinance current-month price failed: {e}"
        )

    # ------------------------------------------------------------------------
    # Second source: NSE allIndices
    # ------------------------------------------------------------------------

    session = _nse_session()

    try:

        response = session.get(
            NSE_API_URL,
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        for idx in payload.get("data", []):

            name = (
                idx.get("indexSymbol")
                or idx.get("index")
                or ""
            ).strip().upper()

            if name != "NIFTY 50":

                continue

            def get_number(keys, default=None):

                for key in keys:

                    value = idx.get(key)

                    if value is None or value == "":

                        continue

                    try:

                        return float(
                            str(value).replace(",", "")
                        )

                    except (
                        TypeError,
                        ValueError,
                    ):

                        continue

                return default

            last = get_number(
                [
                    "last",
                    "lastPrice",
                    "ltp",
                    "close",
                ]
            )

            if last is None or last <= 0:

                raise RuntimeError(
                    "NSE did not provide a valid NIFTY 50 price."
                )

            open_price = get_number(
                ["open", "openPrice"],
                last,
            )

            high_price = get_number(
                [
                    "dayHigh",
                    "high",
                    "dayHighPrice",
                ],
                last,
            )

            low_price = get_number(
                [
                    "dayLow",
                    "low",
                    "dayLowPrice",
                ],
                last,
            )

            volume = get_number(
                [
                    "totalTradedVolume",
                    "totalTradedVol",
                    "volume",
                ],
                0,
            )

            row = pd.DataFrame([
                {
                    "date": current_month,
                    "open": open_price,
                    "high": max(high_price, last),
                    "low": min(low_price, last),
                    "close": last,
                    "volume": int(volume),
                }
            ])

            log.info(
                f"Current-month price from NSE: {last:.2f}"
            )

            return row

        raise RuntimeError(
            "NIFTY 50 not found in NSE allIndices."
        )

    except Exception as e:

        raise RuntimeError(
            f"Unable to fetch current-month price for "
            f"{month_key}: {e}"
        ) from e


# ============================================================================
# 11. FETCH CURRENT-MONTH LIVE PE
# ============================================================================

def fetch_current_pe() -> tuple[Optional[float], str]:

    log.info(
        "Fetching current-month PE from NSE..."
    )

    session = _nse_session()

    try:

        response = session.get(
            NSE_API_URL,
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        for idx in payload.get("data", []):

            name = (
                idx.get("indexSymbol")
                or idx.get("index")
                or ""
            ).strip().upper()

            if name != "NIFTY 50":

                continue

            pe = (
                idx.get("pe")
                or idx.get("trailingPE")
                or idx.get("PE")
            )

            if pe is None:

                continue

            try:

                pe = float(pe)

            except (
                TypeError,
                ValueError,
            ):

                continue

            if math.isfinite(pe) and pe > 0:

                log.info(
                    f"Current NIFTY 50 PE: {pe:.2f}"
                )

                return pe, "nse_live"

        log.warning(
            "NIFTY 50 PE not found in NSE response."
        )

    except Exception as e:

        log.warning(
            f"NSE live PE request failed: {e}"
        )

    return None, "unavailable"


# ============================================================================
# 12. BUILD FINAL DATAFRAME
# ============================================================================

def build_final_dataframe(
    price_df: pd.DataFrame,
    client: Client,
) -> pd.DataFrame:

    log.info(
        "Building final monthly NIFTY 50 dataset..."
    )

    current_month = get_current_month()

    # ------------------------------------------------------------------------
    # Historical PE from CSV
    # ------------------------------------------------------------------------

    pe_df = load_historical_pe()

    # ------------------------------------------------------------------------
    # Normalize prices
    # ------------------------------------------------------------------------

    price_df = price_df.copy()

    price_df["date"] = normalize_month(
        price_df["date"]
    )

    price_df = (
        price_df
        .dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------------
    # Historical recovery from Supabase
    # ------------------------------------------------------------------------

    earliest_price_month = (
        price_df["date"]
        .min()
        .to_period("M")
        .to_timestamp()
    )

    validation_end = (
        current_month
        - pd.DateOffset(months=1)
    )

    pe_df = recover_historical_pe_from_supabase(
        client=client,
        pe_df=pe_df,
        start_date=earliest_price_month,
        end_date=validation_end,
    )

    # ------------------------------------------------------------------------
    # Historical coverage validation
    # ------------------------------------------------------------------------

    validate_historical_pe_coverage(
        price_df,
        pe_df,
    )

    # ------------------------------------------------------------------------
    # Remove current-month price bar from monthly history
    # ------------------------------------------------------------------------

    historical_price_df = price_df[
        price_df["date"] != current_month
    ].copy()

    # ------------------------------------------------------------------------
    # Current-month price
    # ------------------------------------------------------------------------

    current_price_df = fetch_current_month_price()

    # ------------------------------------------------------------------------
    # Current-month PE
    # ------------------------------------------------------------------------

    current_pe, current_source = fetch_current_pe()

    if current_pe is None:

        raise RuntimeError(
            f"Unable to fetch current-month PE for "
            f"{current_month.strftime('%Y-%m')}. "
            "Workflow stopped to prevent NULL insertion."
        )

    # ------------------------------------------------------------------------
    # Create current-month PE row
    # ------------------------------------------------------------------------

    current_pe_row = pd.DataFrame([
        {
            "date": current_month,
            "pe_ratio": float(current_pe),
            "pe_source": current_source,
        }
    ])

    # ------------------------------------------------------------------------
    # Append current-month data
    # ------------------------------------------------------------------------

    final_price_df = pd.concat(
        [
            historical_price_df,
            current_price_df,
        ],
        ignore_index=True,
    )

    final_price_df = (
        final_price_df
        .sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    final_pe_df = pd.concat(
        [
            pe_df[
                pe_df["date"] != current_month
            ],
            current_pe_row,
        ],
        ignore_index=True,
    )

    final_pe_df = (
        final_pe_df
        .sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------------

    merged = final_price_df.merge(
        final_pe_df,
        on="date",
        how="left",
        validate="one_to_one",
    )

    # ------------------------------------------------------------------------
    # Validate PE
    # ------------------------------------------------------------------------

    invalid_pe = (
        merged["pe_ratio"].isna()
        | (merged["pe_ratio"] <= 0)
        | (~np.isfinite(merged["pe_ratio"]))
    )

    if invalid_pe.any():

        missing_months = (
            merged.loc[invalid_pe, "date"]
            .dt.strftime("%Y-%m")
            .tolist()
        )

        raise RuntimeError(
            "Invalid or missing PE for: "
            + ", ".join(missing_months)
        )

    # ------------------------------------------------------------------------
    # Calculate EPS
    # ------------------------------------------------------------------------

    merged["eps_ttm"] = (
        merged["close"] / merged["pe_ratio"]
    ).round(4)

    invalid_eps = (
        merged["eps_ttm"].isna()
        | (merged["eps_ttm"] <= 0)
        | (~np.isfinite(merged["eps_ttm"]))
    )

    if invalid_eps.any():

        bad_months = (
            merged.loc[invalid_eps, "date"]
            .dt.strftime("%Y-%m")
            .tolist()
        )

        raise RuntimeError(
            "Invalid EPS for: "
            + ", ".join(bad_months)
        )

    # ------------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------------

    merged["ticker"] = TICKER

    merged["updated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    merged["pe_source"] = (
        merged["pe_source"]
        .fillna("unavailable")
    )

    # ------------------------------------------------------------------------
    # Final report
    # ------------------------------------------------------------------------

    log.info("")
    log.info("=" * 65)
    log.info("FINAL DATASET SUMMARY")
    log.info("=" * 65)

    log.info(
        f"Total records: {len(merged)}"
    )

    log.info(
        f"Valid PE: {merged['pe_ratio'].notna().sum()}"
    )

    log.info(
        f"Valid EPS: {merged['eps_ttm'].notna().sum()}"
    )

    log.info(
        f"PE source distribution: "
        f"{merged['pe_source'].value_counts().to_dict()}"
    )

    log.info(
        "\nLatest 10 records:\n"
        + merged.tail(10).to_string(index=False)
    )

    log.info("=" * 65)

    return merged[
        [
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "pe_ratio",
            "eps_ttm",
            "pe_source",
            "updated_at",
        ]
    ]


# ============================================================================
# 13. JSON SAFE CLEANING
# ============================================================================

def _clean_row(row: dict) -> dict:

    out = {}

    for key, value in row.items():

        if value is None:

            out[key] = None

        elif isinstance(value, (pd.Timestamp, datetime)):

            out[key] = value.isoformat()

        elif isinstance(value, (np.integer,)):

            out[key] = int(value)

        elif isinstance(value, (np.floating,)):

            value = float(value)

            out[key] = (
                value if math.isfinite(value) else None
            )

        elif isinstance(value, float):

            out[key] = (
                value if math.isfinite(value) else None
            )

        elif isinstance(value, np.bool_):

            out[key] = bool(value)

        else:

            out[key] = value

    return out


# ============================================================================
# 14. UPSERT TO SUPABASE
# ============================================================================

def upsert_to_supabase(
    df: pd.DataFrame,
    client: Client,
) -> None:

    records = df.copy()

    records["date"] = (
        pd.to_datetime(records["date"])
        .dt.strftime("%Y-%m-%d")
    )

    records["volume"] = (
        pd.to_numeric(
            records["volume"],
            errors="coerce",
        )
        .fillna(0)
        .astype("int64")
    )

    records["pe_ratio"] = pd.to_numeric(
        records["pe_ratio"],
        errors="coerce",
    )

    records["eps_ttm"] = pd.to_numeric(
        records["eps_ttm"],
        errors="coerce",
    )

    # Final safety check BEFORE database write.

    if records[
        [
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "pe_ratio",
            "eps_ttm",
        ]
    ].isna().any().any():

        raise RuntimeError(
            "NULL values found in mandatory fields. "
            "Supabase upsert cancelled."
        )

    if (
        (records["pe_ratio"] <= 0).any()
        or (records["eps_ttm"] <= 0).any()
    ):

        raise RuntimeError(
            "Invalid PE/EPS detected. "
            "Supabase upsert cancelled."
        )

    rows = [
        _clean_row(row)
        for row in records.to_dict(
            orient="records"
        )
    ]

    total = len(rows)

    log.info(
        f"Preparing {total} rows for Supabase upsert..."
    )

    for i in range(0, total, BATCH_SIZE):

        batch = rows[
            i:i + BATCH_SIZE
        ]

        (
            client
            .table(TABLE_NAME)
            .upsert(
                batch,
                on_conflict="date,ticker",
            )
            .execute()
        )

        log.info(
            f"Upserted "
            f"{min(i + BATCH_SIZE, total)}/{total} rows."
        )

    log.info(
        f"Successfully upserted {total} records "
        f"into {TABLE_NAME}."
    )


# ============================================================================
# 15. MAIN EXECUTION
# ============================================================================

def run() -> None:

    log.info("=" * 70)

    log.info(
        "NIFTY 50 MONTHLY P/E TRACKER V8 - STARTING"
    )

    log.info("=" * 70)

    log.info(
        f"India date: {datetime.now(IST).isoformat()}"
    )

    # ------------------------------------------------------------------------
    # Supabase connection
    # ------------------------------------------------------------------------

    log.info(
        "Connecting to Supabase..."
    )

    supabase = create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
    )

    log.info(
        "Supabase connection initialized."
    )

    # ------------------------------------------------------------------------
    # Fetch monthly prices
    # ------------------------------------------------------------------------

    price_df = fetch_monthly_price()

    # ------------------------------------------------------------------------
    # Build final dataset
    # ------------------------------------------------------------------------

    final_df = build_final_dataframe(
        price_df,
        supabase,
    )

    # ------------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------------

    log.info("")
    log.info("=" * 70)
    log.info("PRE-UPSERT VALIDATION")
    log.info("=" * 70)

    log.info(
        f"Total records: {len(final_df)}"
    )

    log.info(
        f"PE coverage: "
        f"{final_df['pe_ratio'].notna().sum()}/{len(final_df)}"
    )

    log.info(
        f"EPS coverage: "
        f"{final_df['eps_ttm'].notna().sum()}/{len(final_df)}"
    )

    log.info(
        f"Sources: "
        f"{final_df['pe_source'].value_counts().to_dict()}"
    )

    # ------------------------------------------------------------------------
    # Ensure completed months contain valid PE
    # ------------------------------------------------------------------------

    current_month = get_current_month()

    completed_null_pe = final_df[
        (final_df["date"] < current_month)
        & (
            final_df["pe_ratio"].isna()
            | (final_df["pe_ratio"] <= 0)
        )
    ]

    if not completed_null_pe.empty:

        raise RuntimeError(
            "Completed month has invalid PE. "
            "Upsert cancelled."
        )

    # ------------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------------

    log.info(
        "All validations passed. Starting Supabase upsert..."
    )

    upsert_to_supabase(
        final_df,
        supabase,
    )

    log.info("=" * 70)

    log.info(
        "NIFTY 50 MONTHLY P/E TRACKER V8 - COMPLETED SUCCESSFULLY"
    )

    log.info("=" * 70)


# ============================================================================
# 16. ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    try:

        run()

    except Exception:

        log.exception(
            "NIFTY 50 P/E TRACKER FAILED."
        )

        sys.exit(1)
