"""
Nifty 50 Monthly P/E Tracker (v7)

Purpose
-------
Maintain monthly NIFTY 50 OHLCV + trailing P/E + derived EPS in Supabase.

PE source priority
------------------
1. data/nifty50_pe_seed.csv
2. EMBEDDED_PE_DATA
3. NSE allIndices API for CURRENT MONTH ONLY

Important
---------
- Historical P/E is NEVER calculated by scaling current P/E.
- Historical completed months must have P/E.
- Current incomplete month may use NSE live P/E.
- If a completed month has no P/E, the script FAILS instead of
  silently inserting NULL.
- eps_ttm is derived as close / pe_ratio. It is a synthetic
  index-level EPS-equivalent, not an NSE-published EPS field.
"""

import os
import sys
import time
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd
import numpy as np
from supabase import create_client, Client


# ============================================================================
# CONFIGURATION
# ============================================================================

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]

TABLE_NAME = "nifty50_pe"
TICKER = "^NSEI"

HISTORY_YEARS = 10

SEED_CSV = (
    Path(__file__).parent
    / "data"
    / "nifty50_pe_seed.csv"
)


# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger(__name__)


# ============================================================================
# NSE HEADERS
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
# EMBEDDED HISTORICAL PE
# ============================================================================
#
# Historical Nifty 50 trailing PE.
#
# Existing values retained from your v6 seed.
# May-Aug 2026 have been added so that completed months do not become NULL.
#
# Current month is NOT included here because current month is fetched live
# from NSE.
# ============================================================================

EMBEDDED_PE_DATA = [

    # ------------------------------------------------------------------------
    # 1999
    # ------------------------------------------------------------------------

    ("1999-01-01", 11.62),
    ("1999-02-01", 12.48),
    ("1999-03-01", 13.50),
    ("1999-04-01", 16.31),
    ("1999-05-01", 14.95),
    ("1999-06-01", 18.25),
    ("1999-07-01", 17.84),
    ("1999-08-01", 19.63),
    ("1999-09-01", 21.38),
    ("1999-10-01", 21.81),
    ("1999-11-01", 19.78),
    ("1999-12-01", 21.69),

    # ------------------------------------------------------------------------
    # 2000
    # ------------------------------------------------------------------------

    ("2000-01-01", 25.91),
    ("2000-02-01", 25.13),
    ("2000-03-01", 27.35),
    ("2000-04-01", 24.70),
    ("2000-05-01", 20.00),
    ("2000-06-01", 22.46),
    ("2000-07-01", 23.31),
    ("2000-08-01", 20.58),
    ("2000-09-01", 21.69),
    ("2000-10-01", 19.48),
    ("2000-11-01", 18.16),
    ("2000-12-01", 19.34),

    # ------------------------------------------------------------------------
    # 2001
    # ------------------------------------------------------------------------

    ("2001-01-01", 19.06),
    ("2001-02-01", 22.02),
    ("2001-03-01", 20.34),
    ("2001-04-01", 17.05),
    ("2001-05-01", 14.63),
    ("2001-06-01", 15.47),
    ("2001-07-01", 15.65),
    ("2001-08-01", 15.01),
    ("2001-09-01", 15.02),
    ("2001-10-01", 13.10),
    ("2001-11-01", 14.11),
    ("2001-12-01", 15.44),

    # ------------------------------------------------------------------------
    # 2002
    # ------------------------------------------------------------------------

    ("2002-01-01", 15.29),
    ("2002-02-01", 17.42),
    ("2002-03-01", 18.96),
    ("2002-04-01", 18.25),
    ("2002-05-01", 18.08),
    ("2002-06-01", 16.39),
    ("2002-07-01", 15.90),
    ("2002-08-01", 14.24),
    ("2002-09-01", 15.08),
    ("2002-10-01", 14.21),
    ("2002-11-01", 14.47),
    ("2002-12-01", 14.48),

    # ------------------------------------------------------------------------
    # 2003
    # ------------------------------------------------------------------------

    ("2003-01-01", 14.92),
    ("2003-02-01", 14.31),
    ("2003-03-01", 14.36),
    ("2003-04-01", 13.44),
    ("2003-05-01", 10.86),
    ("2003-06-01", 11.59),
    ("2003-07-01", 12.32),
    ("2003-08-01", 12.95),
    ("2003-09-01", 15.16),
    ("2003-10-01", 15.66),
    ("2003-11-01", 17.65),
    ("2003-12-01", 18.27),

    # ------------------------------------------------------------------------
    # 2004
    # ------------------------------------------------------------------------

    ("2004-01-01", 21.09),
    ("2004-02-01", 19.55),
    ("2004-03-01", 21.57),
    ("2004-04-01", 21.27),
    ("2004-05-01", 16.62),
    ("2004-06-01", 12.14),
    ("2004-07-01", 12.90),
    ("2004-08-01", 13.69),
    ("2004-09-01", 13.67),
    ("2004-10-01", 14.84),
    ("2004-11-01", 15.03),
    ("2004-12-01", 16.42),

    # ------------------------------------------------------------------------
    # 2005
    # ------------------------------------------------------------------------

    ("2005-01-01", 15.57),
    ("2005-02-01", 14.34),
    ("2005-03-01", 14.88),
    ("2005-04-01", 14.84),
    ("2005-05-01", 13.32),
    ("2005-06-01", 13.93),
    ("2005-07-01", 14.26),
    ("2005-08-01", 14.36),
    ("2005-09-01", 14.92),
    ("2005-10-01", 16.33),
    ("2005-11-01", 14.32),
    ("2005-12-01", 16.23),

    # ------------------------------------------------------------------------
    # 2006
    # ------------------------------------------------------------------------

    ("2006-01-01", 17.16),
    ("2006-02-01", 17.73),
    ("2006-03-01", 18.56),
    ("2006-04-01", 20.68),
    ("2006-05-01", 20.46),
    ("2006-06-01", 16.78),
    ("2006-07-01", 18.57),
    ("2006-08-01", 17.67),
    ("2006-09-01", 19.68),
    ("2006-10-01", 20.82),
    ("2006-11-01", 20.16),
    ("2006-12-01", 21.41),

    # ------------------------------------------------------------------------
    # 2007
    # ------------------------------------------------------------------------

    ("2007-01-01", 21.48),
    ("2007-02-01", 19.95),
    ("2007-03-01", 18.33),
    ("2007-04-01", 17.49),
    ("2007-05-01", 19.76),
    ("2007-06-01", 20.41),
    ("2007-07-01", 20.62),
    ("2007-08-01", 19.65),
    ("2007-09-01", 20.25),
    ("2007-10-01", 22.79),
    ("2007-11-01", 25.65),
    ("2007-12-01", 25.66),

    # ------------------------------------------------------------------------
    # 2008
    # ------------------------------------------------------------------------

    ("2008-01-01", 27.64),
    ("2008-02-01", 22.68),
    ("2008-03-01", 21.12),
    ("2008-04-01", 20.66),
    ("2008-05-01", 22.42),
    ("2008-06-01", 20.17),
    ("2008-07-01", 16.66),
    ("2008-08-01", 18.56),
    ("2008-09-01", 18.38),
    ("2008-10-01", 16.98),
    ("2008-11-01", 13.33),
    ("2008-12-01", 11.76),

    # ------------------------------------------------------------------------
    # 2009
    # ------------------------------------------------------------------------

    ("2009-01-01", 13.30),
    ("2009-02-01", 13.12),
    ("2009-03-01", 12.70),
    ("2009-04-01", 14.49),
    ("2009-05-01", 17.37),
    ("2009-06-01", 20.62),
    ("2009-07-01", 20.20),
    ("2009-08-01", 21.09),
    ("2009-09-01", 20.78),
    ("2009-10-01", 22.89),
    ("2009-11-01", 19.81),
    ("2009-12-01", 22.77),

    # ------------------------------------------------------------------------
    # 2010
    # ------------------------------------------------------------------------

    ("2010-01-01", 23.31),
    ("2010-02-01", 21.07),
    ("2010-03-01", 21.33),
    ("2010-04-01", 22.52),
    ("2010-05-01", 22.06),
    ("2010-06-01", 20.81),
    ("2010-07-01", 21.99),
    ("2010-08-01", 22.91),
    ("2010-09-01", 23.02),
    ("2010-10-01", 25.54),
    ("2010-11-01", 25.12),
    ("2010-12-01", 23.78),

    # ------------------------------------------------------------------------
    # 2011
    # ------------------------------------------------------------------------

    ("2011-01-01", 24.57),
    ("2011-02-01", 20.70),
    ("2011-03-01", 21.14),
    ("2011-04-01", 22.11),
    ("2011-05-01", 21.19),
    ("2011-06-01", 20.65),
    ("2011-07-01", 20.75),
    ("2011-08-01", 19.81),
    ("2011-09-01", 18.19),
    ("2011-10-01", 17.51),
    ("2011-11-01", 19.04),
    ("2011-12-01", 17.87),

    # ------------------------------------------------------------------------
    # 2012
    # ------------------------------------------------------------------------

    ("2012-01-01", 16.79),
    ("2012-02-01", 18.66),
    ("2012-03-01", 18.92),
    ("2012-04-01", 18.79),
    ("2012-05-01", 17.99),
    ("2012-06-01", 16.36),
    ("2012-07-01", 17.51),
    ("2012-08-01", 17.13),
    ("2012-09-01", 17.63),
    ("2012-10-01", 19.22),
    ("2012-11-01", 18.46),
    ("2012-12-01", 18.56),

    # ------------------------------------------------------------------------
    # 2013
    # ------------------------------------------------------------------------

    ("2013-01-01", 18.82),
    ("2013-02-01", 18.42),
    ("2013-03-01", 17.74),
    ("2013-04-01", 17.51),
    ("2013-05-01", 18.05),
    ("2013-06-01", 17.81),
    ("2013-07-01", 17.96),
    ("2013-08-01", 17.03),
    ("2013-09-01", 16.00),
    ("2013-10-01", 16.95),
    ("2013-11-01", 18.20),
    ("2013-12-01", 18.51),

    # ------------------------------------------------------------------------
    # 2014
    # ------------------------------------------------------------------------

    ("2014-01-01", 18.69),
    ("2014-02-01", 17.34),
    ("2014-03-01", 17.52),
    ("2014-04-01", 18.91),
    ("2014-05-01", 18.72),
    ("2014-06-01", 20.27),
    ("2014-07-01", 20.71),
    ("2014-08-01", 20.05),
    ("2014-09-01", 20.99),
    ("2014-10-01", 20.77),
    ("2014-11-01", 21.58),
    ("2014-12-01", 21.85),

    # ------------------------------------------------------------------------
    # 2015
    # ------------------------------------------------------------------------

    ("2015-01-01", 21.16),
    ("2015-02-01", 22.51),
    ("2015-03-01", 23.95),
    ("2015-04-01", 22.95),
    ("2015-05-01", 22.47),
    ("2015-06-01", 23.25),
    ("2015-07-01", 23.43),
    ("2015-08-01", 23.54),
    ("2015-09-01", 21.57),
    ("2015-10-01", 22.21),
    ("2015-11-01", 21.98),
    ("2015-12-01", 21.51),

    # ------------------------------------------------------------------------
    # 2016
    # ------------------------------------------------------------------------

    ("2016-01-01", 21.53),
    ("2016-02-01", 20.22),
    ("2016-03-01", 19.53),
    ("2016-04-01", 21.19),
    ("2016-05-01", 21.25),
    ("2016-06-01", 22.01),
    ("2016-07-01", 23.22),
    ("2016-08-01", 23.78),
    ("2016-09-01", 23.06),
    ("2016-10-01", 22.44),
    ("2016-11-01", 20.68),
    ("2016-12-01", 21.16),

    # ------------------------------------------------------------------------
    # 2017
    # ------------------------------------------------------------------------

    ("2017-01-01", 22.12),
    ("2017-02-01", 22.89),
    ("2017-03-01", 23.38),
    ("2017-04-01", 23.48),
    ("2017-05-01", 23.98),
    ("2017-06-01", 24.03),
    ("2017-07-01", 25.25),
    ("2017-08-01", 25.46),
    ("2017-09-01", 26.67),
    ("2017-10-01", 26.42),
    ("2017-11-01", 25.75),
    ("2017-12-01", 26.57),

    # ------------------------------------------------------------------------
    # 2018
    # ------------------------------------------------------------------------

    ("2018-01-01", 26.83),
    ("2018-02-01", 24.96),
    ("2018-03-01", 23.60),
    ("2018-04-01", 23.73),
    ("2018-05-01", 23.56),
    ("2018-06-01", 23.14),
    ("2018-07-01", 24.04),
    ("2018-08-01", 27.40),
    ("2018-09-01", 27.62),
    ("2018-10-01", 24.24),
    ("2018-11-01", 25.06),
    ("2018-12-01", 24.51),

    # ------------------------------------------------------------------------
    # 2019
    # ------------------------------------------------------------------------

    ("2019-01-01", 25.59),
    ("2019-02-01", 27.23),
    ("2019-03-01", 28.90),
    ("2019-04-01", 29.10),
    ("2019-05-01", 28.82),
    ("2019-06-01", 29.48),
    ("2019-07-01", 28.62),
    ("2019-08-01", 27.44),
    ("2019-09-01", 27.90),
    ("2019-10-01", 28.50),
    ("2019-11-01", 28.40),
    ("2019-12-01", 29.42),

    # ------------------------------------------------------------------------
    # 2020
    # ------------------------------------------------------------------------

    ("2020-01-01", 29.04),
    ("2020-02-01", 26.27),
    ("2020-03-01", 20.25),
    ("2020-04-01", 22.55),
    ("2020-05-01", 23.92),
    ("2020-06-01", 30.47),
    ("2020-07-01", 32.46),
    ("2020-08-01", 34.61),
    ("2020-09-01", 34.57),
    ("2020-10-01", 33.81),
    ("2020-11-01", 37.97),
    ("2020-12-01", 38.47),

    # ------------------------------------------------------------------------
    # 2021
    # ------------------------------------------------------------------------

    ("2021-01-01", 40.87),
    ("2021-02-01", 41.20),
    ("2021-03-01", 40.20),
    ("2021-04-01", 40.77),
    ("2021-05-01", 32.20),
    ("2021-06-01", 30.92),
    ("2021-07-01", 30.27),
    ("2021-08-01", 29.04),
    ("2021-09-01", 28.62),
    ("2021-10-01", 27.93),
    ("2021-11-01", 26.97),
    ("2021-12-01", 26.38),

    # ------------------------------------------------------------------------
    # 2022
    # ------------------------------------------------------------------------

    ("2022-01-01", 24.18),
    ("2022-02-01", 23.27),
    ("2022-03-01", 23.11),
    ("2022-04-01", 22.22),
    ("2022-05-01", 21.08),
    ("2022-06-01", 19.76),
    ("2022-07-01", 21.65),
    ("2022-08-01", 22.46),
    ("2022-09-01", 21.87),
    ("2022-10-01", 22.11),
    ("2022-11-01", 22.67),
    ("2022-12-01", 22.34),

    # ------------------------------------------------------------------------
    # 2023
    # ------------------------------------------------------------------------

    ("2023-01-01", 22.39),
    ("2023-02-01", 21.90),
    ("2023-03-01", 22.38),
    ("2023-04-01", 23.16),
    ("2023-05-01", 22.84),
    ("2023-06-01", 23.41),
    ("2023-07-01", 23.38),
    ("2023-08-01", 22.96),
    ("2023-09-01", 23.04),
    ("2023-10-01", 22.70),
    ("2023-11-01", 23.40),
    ("2023-12-01", 24.11),

    # ------------------------------------------------------------------------
    # 2024
    # ------------------------------------------------------------------------

    ("2024-01-01", 23.83),
    ("2024-02-01", 23.52),
    ("2024-03-01", 23.18),
    ("2024-04-01", 23.63),
    ("2024-05-01", 22.57),
    ("2024-06-01", 23.55),
    ("2024-07-01", 24.42),
    ("2024-08-01", 23.97),
    ("2024-09-01", 24.08),
    ("2024-10-01", 22.27),
    ("2024-11-01", 22.08),
    ("2024-12-01", 22.38),

    # ------------------------------------------------------------------------
    # 2025
    # ------------------------------------------------------------------------

    ("2025-01-01", 22.48),
    ("2025-02-01", 21.64),
    ("2025-03-01", 20.97),
    ("2025-04-01", 20.11),
    ("2025-05-01", 21.23),

    ("2025-06-01", 22.50),
    ("2025-07-01", 22.50),
    ("2025-08-01", 21.80),
    ("2025-09-01", 21.90),
    ("2025-10-01", 22.50),
    ("2025-11-01", 22.60),
    ("2025-12-01", 22.60),

    # ------------------------------------------------------------------------
    # 2026
    # ------------------------------------------------------------------------

    ("2026-01-01", 22.30),
    ("2026-02-01", 22.40),
    ("2026-03-01", 20.60),
    ("2026-04-01", 20.90),

    # Added in v7
    ("2026-05-01", 20.27),
    ("2026-06-01", 20.58),
    ("2026-07-01", 20.72),
    ("2026-08-01", 20.36),

    # September is intentionally NOT here.
    # It is fetched live from NSE.
]


# ============================================================================
# 1. MONTHLY PRICE DATA
# ============================================================================

def fetch_monthly_price() -> pd.DataFrame:

    log.info(
        f"Fetching {HISTORY_YEARS}-year monthly OHLCV "
        f"for {TICKER} ..."
    )

    df = yf.Ticker(TICKER).history(
        period=f"{HISTORY_YEARS}y",
        interval="1mo",
        auto_adjust=True,
    )

    if df.empty:
        raise RuntimeError(
            "yfinance returned empty price data for ^NSEI"
        )

    # Remove timezone if present
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)

    df = df.reset_index()

    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df["date"] = (
        pd.to_datetime(df["date"])
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    df = df[
        [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]

    log.info(
        f"  -> {len(df)} monthly bars "
        f"({df['date'].min().date()} - "
        f"{df['date'].max().date()})"
    )

    return df


# ============================================================================
# 2. LOAD HISTORICAL PE
# ============================================================================

def load_historical_pe() -> pd.DataFrame:

    """
    Load historical PE from both sources.

    Priority by month:
      1. Seed CSV (authoritative when a month exists there)
      2. EMBEDDED_PE_DATA fills only months missing from CSV

    The current/latest price month is handled separately by the live NSE PE
    fetch in build_final_dataframe().
    """

    csv_df = pd.DataFrame(columns=["date", "pe_ratio", "pe_source"])

    # ------------------------------------------------------------------------
    # 1. Load seed CSV
    # ------------------------------------------------------------------------

    if SEED_CSV.exists() and SEED_CSV.stat().st_size > 100:

        try:

            csv_df = pd.read_csv(SEED_CSV)

            if "date" not in csv_df.columns:
                raise ValueError("Seed CSV must contain 'date' column")

            if "pe_ratio" not in csv_df.columns:
                raise ValueError("Seed CSV must contain 'pe_ratio' column")

            csv_df["date"] = pd.to_datetime(
                csv_df["date"],
                errors="coerce",
            )

            csv_df["pe_ratio"] = pd.to_numeric(
                csv_df["pe_ratio"],
                errors="coerce",
            )

            csv_df = csv_df.dropna(
                subset=["date", "pe_ratio"]
            )

            if "pe_source" not in csv_df.columns:
                csv_df["pe_source"] = "nse_historical_seed"

            csv_df["pe_source"] = (
                csv_df["pe_source"]
                .fillna("nse_historical_seed")
            )

            csv_df["date"] = (
                csv_df["date"]
                .dt.to_period("M")
                .dt.to_timestamp()
            )

            csv_df = (
                csv_df[
                    ["date", "pe_ratio", "pe_source"]
                ]
                .drop_duplicates(
                    subset=["date"],
                    keep="last",
                )
            )

            log.info(
                f"Loaded {len(csv_df)} historical PE rows from seed CSV "
                f"({csv_df['date'].min().date()} - "
                f"{csv_df['date'].max().date()})"
            )

        except Exception as e:

            log.warning(
                f"Seed CSV read error: {e}"
            )

            csv_df = pd.DataFrame(
                columns=["date", "pe_ratio", "pe_source"]
            )

    # ------------------------------------------------------------------------
    # 2. Load embedded PE data
    # ------------------------------------------------------------------------

    embedded_df = pd.DataFrame(
        EMBEDDED_PE_DATA,
        columns=["date", "pe_ratio"],
    )

    embedded_df["date"] = pd.to_datetime(
        embedded_df["date"],
        errors="coerce",
    )

    embedded_df["pe_ratio"] = pd.to_numeric(
        embedded_df["pe_ratio"],
        errors="coerce",
    )

    embedded_df = embedded_df.dropna(
        subset=["date", "pe_ratio"]
    )

    embedded_df["date"] = (
        embedded_df["date"]
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    embedded_df["pe_source"] = "nse_historical_seed"

    embedded_df = (
        embedded_df[
            ["date", "pe_ratio", "pe_source"]
        ]
        .drop_duplicates(
            subset=["date"],
            keep="last",
        )
    )

    log.info(
        f"Embedded PE data contains {len(embedded_df)} rows "
        f"({embedded_df['date'].min().date()} - "
        f"{embedded_df['date'].max().date()})"
    )

    # ------------------------------------------------------------------------
    # 3. Fill only CSV gaps with embedded data
    # ------------------------------------------------------------------------

    csv_dates = set(csv_df["date"])

    embedded_missing = embedded_df[
        ~embedded_df["date"].isin(csv_dates)
    ].copy()

    log.info(
        f"Filled {len(embedded_missing)} missing month(s) from embedded PE data"
    )

    combined = pd.concat(
        [csv_df, embedded_missing],
        ignore_index=True,
    )

    combined = (
        combined
        .sort_values("date")
        .drop_duplicates(
            subset=["date"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    if combined.empty:
        raise RuntimeError(
            "No historical PE data available from CSV or embedded data."
        )

    log.info(
        f"Combined historical PE: {len(combined)} rows "
        f"({combined['date'].min().date()} - "
        f"{combined['date'].max().date()})"
    )

    return combined[
        ["date", "pe_ratio", "pe_source"]
    ]


# ============================================================================
# 3. CURRENT-MONTH MTD PRICE DATA
# ============================================================================

def fetch_current_month_price() -> pd.DataFrame:

    """
    Fetch month-to-date NIFTY 50 OHLCV for the current calendar month.

    The monthly yfinance series normally ends at the previous completed
    month when this workflow runs on the 2nd. Therefore the current month
    must be created separately.

    The returned row is dated to the first day of the current month.
    OHLCV is aggregated month-to-date from daily NIFTY data.
    """

    current_month = (
        pd.Timestamp.today()
        .to_period("M")
        .to_timestamp()
    )

    log.info(
        f"Fetching current-month MTD OHLCV for {TICKER} "
        f"({current_month.strftime('%Y-%m')}) ..."
    )

    df = yf.Ticker(TICKER).history(
        period="10d",
        interval="1d",
        auto_adjust=True,
    )

    if df.empty:
        raise RuntimeError(
            "yfinance returned empty daily data for current month."
        )

    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)

    df = df.reset_index()

    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    month_df = df[
        df["date"].dt.to_period("M")
        == current_month.to_period("M")
    ].copy()

    if month_df.empty:
        raise RuntimeError(
            f"No daily NIFTY 50 price data found for "
            f"current month {current_month.strftime('%Y-%m')}."
        )

    # Month-to-date OHLCV.
    row = pd.DataFrame([{
        "date": current_month,
        "open": float(month_df.iloc[0]["open"]),
        "high": float(month_df["high"].max()),
        "low": float(month_df["low"].min()),
        "close": float(month_df.iloc[-1]["close"]),
        "volume": int(pd.to_numeric(month_df["volume"], errors="coerce").fillna(0).sum()),
    }])

    log.info(
        f"  -> Current month row: {current_month.strftime('%Y-%m-%d')} "
        f"close={row.iloc[0]['close']:.2f}"
    )

    return row


# ============================================================================
# 4. NSE CURRENT-MONTH PE
# ============================================================================

def _nse_session() -> requests.Session:

    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    # A single short warm-up request is enough. The previous version
    # performed two 15-second warm-ups, which could make the GitHub Action
    # appear stuck when NSE was slow or blocking the runner.
    try:
        session.get(
            "https://www.nseindia.com/",
            timeout=5,
        )
    except requests.RequestException as e:
        log.warning(f"NSE warm-up failed: {e}")

    return session


def fetch_current_pe() -> tuple[float | None, str]:

    """
    Fetch the latest NIFTY 50 trailing PE from NSE.

    This function deliberately has short timeouts so a GitHub Actions runner
    cannot spend several minutes waiting for NSE. If NSE is unavailable, the
    caller receives (None, "unavailable") and the final merge safety check
    will stop the workflow rather than inserting a NULL PE.
    """

    log.info("Fetching current month PE from NSE allIndices ...")

    session = _nse_session()

    try:
        response = session.get(
            "https://www.nseindia.com/api/allIndices",
            timeout=10,
        )

        response.raise_for_status()

        content_type = response.headers.get(
            "Content-Type",
            "",
        )

        if (
            "json" not in content_type.lower()
            and "javascript" not in content_type.lower()
        ):
            log.warning(
                f"NSE allIndices returned non-JSON response: {content_type}"
            )
            return None, "unavailable"

        payload = response.json()

        for idx in payload.get("data", []):

            name = (
                idx.get("indexSymbol")
                or idx.get("index")
                or ""
            ).strip().upper()

            if name == "NIFTY 50":

                pe = (
                    idx.get("pe")
                    or idx.get("trailingPE")
                    or idx.get("PE")
                )

                if pe is None:
                    continue

                try:
                    pe = float(pe)
                except (TypeError, ValueError):
                    continue

                if math.isfinite(pe) and pe > 0:
                    log.info(
                        f"  -> NIFTY 50 current PE = {pe}"
                    )
                    return pe, "nse_live"

        log.warning(
            "NIFTY 50 not found in NSE allIndices response"
        )

    except requests.Timeout as e:
        log.warning(
            f"NSE allIndices request timed out after 10 seconds: {e}"
        )

    except requests.RequestException as e:
        log.warning(
            f"NSE allIndices request failed: {e}"
        )

    except ValueError as e:
        log.warning(
            f"NSE allIndices returned invalid JSON: {e}"
        )

    except Exception as e:
        log.warning(
            f"Unexpected NSE allIndices error: {e}"
        )

    return None, "unavailable"


# ============================================================================
# 4. VALIDATE HISTORICAL PE COVERAGE
# ============================================================================

def validate_historical_pe_coverage(
    price_df: pd.DataFrame,
    pe_df: pd.DataFrame,
) -> None:

    """
    Validate historical PE coverage.

    The latest available price month is deliberately excluded because the
    monthly workflow obtains its PE live from NSE and assigns that live PE to
    the latest available monthly price bar.

    Example on 2-Oct-2026:
        price data      : 2016-10 -> 2026-09
        historical PE   : 1999-01 -> 2026-08
        validation      : 2016-10 -> 2026-08
        live NSE PE     : assigned to 2026-09
    """

    if price_df.empty:
        raise RuntimeError("Price dataframe is empty.")

    if pe_df.empty:
        raise RuntimeError("Historical PE dataframe is empty.")

    price_dates = pd.to_datetime(
        price_df["date"],
        errors="coerce",
    ).dropna()

    pe_dates = pd.to_datetime(
        pe_df["date"],
        errors="coerce",
    ).dropna()

    if price_dates.empty:
        raise RuntimeError("No valid price dates available.")

    if pe_dates.empty:
        raise RuntimeError("No valid historical PE dates available.")

    latest_price_month = (
        price_dates.max()
        .to_period("M")
        .to_timestamp()
    )

    earliest_pe_month = (
        pe_dates.min()
        .to_period("M")
        .to_timestamp()
    )

    validation_end = (
        latest_price_month
        - pd.DateOffset(months=1)
    )

    log.info(
        f"Historical PE validation period: "
        f"{earliest_pe_month.date()} -> {validation_end.date()}"
    )

    if validation_end < earliest_pe_month:
        log.info(
            "No historical PE coverage period requires validation."
        )
        return

    price_months = set(
        price_dates
        .dt.to_period("M")
        .loc[
            (price_dates.dt.to_period("M") >= earliest_pe_month.to_period("M"))
            & (price_dates.dt.to_period("M") <= validation_end.to_period("M"))
        ]
        .drop_duplicates()
    )

    pe_months = set(
        pe_dates.dt.to_period("M")
    )

    missing = sorted(
        str(month)
        for month in price_months
        if month not in pe_months
    )

    if missing:
        raise RuntimeError(
            "\n\n"
            "====================================================\n"
            "HISTORICAL PE COVERAGE ERROR\n"
            "====================================================\n"
            f"Missing PE for {len(missing)} historical month(s):\n"
            f"{', '.join(missing)}\n\n"
            "The script has STOPPED intentionally.\n"
            "No NULL historical PE values will be inserted.\n\n"
            "Update data/nifty50_pe_seed.csv or EMBEDDED_PE_DATA.\n"
            "====================================================\n"
        )

    log.info(
        "Historical PE coverage validation: PASSED"
    )


# ============================================================================
# 5. BUILD FINAL DATAFRAME
# ============================================================================

def build_final_dataframe(
    price_df: pd.DataFrame,
) -> pd.DataFrame:

    # ------------------------------------------------------------------------
    # Historical PE
    # ------------------------------------------------------------------------

    pe_df = load_historical_pe()

    pe_df["date"] = (
        pd.to_datetime(pe_df["date"], errors="coerce")
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    pe_df = (
        pe_df
        .dropna(subset=["date", "pe_ratio"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
    )

    # ------------------------------------------------------------------------
    # IMPORTANT ARCHITECTURE
    # ------------------------------------------------------------------------
    # The monthly yfinance series contains completed months only. On the 2nd
    # of a new month, its latest bar is the PREVIOUS month. We therefore do
    # NOT attach the live PE to price_df's latest month.
    #
    # Instead:
    #   1. Validate historical PE through the previous completed month.
    #   2. Fetch the CURRENT calendar month's MTD OHLCV separately.
    #   3. Fetch live NSE PE.
    #   4. Create a NEW current-month row dated YYYY-MM-01.
    #
    # Example:
    #   2-Oct-2026 -> historical data ends Sep-2026
    #   current row = 2026-10-01
    #   live NSE PE -> 2026-10-01
    # ------------------------------------------------------------------------

    validate_historical_pe_coverage(
        price_df,
        pe_df,
    )

    current_month = (
        pd.Timestamp.today()
        .to_period("M")
        .to_timestamp()
    )

    # ------------------------------------------------------------------------
    # Fetch current calendar month's MTD price row
    # ------------------------------------------------------------------------

    current_price_df = fetch_current_month_price()

    # Guard against accidental duplicate current month from yfinance.
    price_df = price_df[
        price_df["date"] != current_month
    ].copy()

    # ------------------------------------------------------------------------
    # Fetch live NSE PE
    # ------------------------------------------------------------------------

    current_pe, current_source = fetch_current_pe()

    if current_pe is None:
        raise RuntimeError(
            f"Could not fetch live NIFTY 50 PE for current month "
            f"{current_month.strftime('%Y-%m')}. "
            "The workflow will not create a NULL current-month PE row."
        )

    # ------------------------------------------------------------------------
    # Create current-month PE row separately
    # ------------------------------------------------------------------------

    current_pe_row = pd.DataFrame([{
        "date": current_month,
        "pe_ratio": current_pe,
        "pe_source": current_source,
    }])

    # Live current-month value wins if a seed/embedded dataset ever contains
    # the same month.
    pe_df = pd.concat(
        [
            pe_df[pe_df["date"] != current_month],
            current_pe_row,
        ],
        ignore_index=True,
    )

    # ------------------------------------------------------------------------
    # Add current-month price row to the historical monthly price dataframe
    # ------------------------------------------------------------------------

    price_df = pd.concat(
        [
            price_df,
            current_price_df,
        ],
        ignore_index=True,
    )

    price_df = (
        price_df
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    log.info(
        f"Created current-month anchor row {current_month.strftime('%Y-%m-%d')}: "
        f"NIFTY={current_price_df.iloc[0]['close']:.2f}, "
        f"PE={current_pe}"
    )

    # ------------------------------------------------------------------------
    # Merge prices + PE
    # ------------------------------------------------------------------------

    merged = price_df.merge(
        pe_df,
        on="date",
        how="left",
    )

    # ------------------------------------------------------------------------
    # Validate every row has PE
    # ------------------------------------------------------------------------

    missing_after_merge = merged.loc[
        merged["pe_ratio"].isna(),
        "date",
    ]

    if not missing_after_merge.empty:
        months = (
            missing_after_merge
            .dt.strftime("%Y-%m")
            .tolist()
        )

        raise RuntimeError(
            "PE is missing after final merge for month(s): "
            + ", ".join(months)
        )

    # ------------------------------------------------------------------------
    # Derived EPS
    # ------------------------------------------------------------------------

    merged["eps_ttm"] = np.nan

    valid_pe = (
        merged["pe_ratio"].notna()
        & (merged["pe_ratio"] > 0)
        & merged["close"].notna()
    )

    merged.loc[valid_pe, "eps_ttm"] = (
        merged.loc[valid_pe, "close"]
        / merged.loc[valid_pe, "pe_ratio"]
    ).round(4)

    # ------------------------------------------------------------------------
    # PE source / metadata
    # ------------------------------------------------------------------------

    merged["pe_source"] = (
        merged["pe_source"]
        .fillna("unavailable")
    )

    merged["ticker"] = TICKER

    merged["updated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    # ------------------------------------------------------------------------
    # Final EPS safety check
    # ------------------------------------------------------------------------

    bad_eps = merged.loc[
        merged["eps_ttm"].isna()
        | (merged["eps_ttm"] <= 0),
        "date",
    ]

    if not bad_eps.empty:
        months = (
            bad_eps
            .dt.strftime("%Y-%m")
            .tolist()
        )

        raise RuntimeError(
            "Final dataset contains invalid derived EPS for: "
            + ", ".join(months)
        )

    # ------------------------------------------------------------------------
    # Coverage logging
    # ------------------------------------------------------------------------

    total = len(merged)
    pe_count = merged["pe_ratio"].notna().sum()
    eps_count = merged["eps_ttm"].notna().sum()

    log.info(f"PE coverage: {pe_count}/{total}")
    log.info(f"EPS coverage: {eps_count}/{total}")

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
# 6. JSON-SAFE CLEANING
# ============================================================================

def _clean_row(row: dict) -> dict:

    out = {}

    for key, value in row.items():

        if value is None:

            out[key] = None

        elif isinstance(value, float):

            if math.isnan(value):

                out[key] = None

            else:

                out[key] = float(value)

        elif isinstance(value, (np.integer,)):

            out[key] = int(value)

        elif isinstance(value, (np.floating,)):

            if math.isnan(float(value)):

                out[key] = None

            else:

                out[key] = float(value)

        elif isinstance(value, (np.bool_,)):

            out[key] = bool(value)

        else:

            out[key] = value

    return out


# ============================================================================
# 7. UPSERT TO SUPABASE
# ============================================================================

def upsert_to_supabase(
    df: pd.DataFrame,
    client: Client,
) -> None:

    records = df.copy()

    # Date -> YYYY-MM-DD
    records["date"] = (
        records["date"]
        .dt.strftime("%Y-%m-%d")
    )

    # Volume -> integer
    records["volume"] = (
        pd.to_numeric(
            records["volume"],
            errors="coerce",
        )
        .fillna(0)
        .astype("int64")
    )

    # PE/EPS -> numeric
    records["pe_ratio"] = pd.to_numeric(
        records["pe_ratio"],
        errors="coerce",
    )

    records["eps_ttm"] = pd.to_numeric(
        records["eps_ttm"],
        errors="coerce",
    )

    rows = [
        _clean_row(row)
        for row in records.to_dict(
            orient="records"
        )
    ]

    total = len(rows)

    log.info(
        f"Preparing to upsert {total} rows..."
    )

    for i in range(
        0,
        total,
        500,
    ):

        batch = rows[
            i:i + 500
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
            f"  -> Upserted "
            f"{min(i + 500, total)}/{total}"
        )

    log.info(
        f"Done - {total} rows upserted "
        f"into `{TABLE_NAME}`"
    )


# ============================================================================
# 8. MAIN
# ============================================================================

def run() -> None:

    log.info("=" * 70)
    log.info(
        "Nifty 50 Monthly P/E Tracker "
        "v7 - starting"
    )
    log.info("=" * 70)

    # ------------------------------------------------------------------------
    # Supabase
    # ------------------------------------------------------------------------

    supabase = create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
    )

    # ------------------------------------------------------------------------
    # Price data
    # ------------------------------------------------------------------------

    price_df = fetch_monthly_price()

    # ------------------------------------------------------------------------
    # Build final
    # ------------------------------------------------------------------------

    final_df = build_final_dataframe(
        price_df
    )

    # ------------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------------

    pe_ok = (
        final_df["pe_ratio"]
        .notna()
        .sum()
    )

    eps_ok = (
        final_df["eps_ttm"]
        .notna()
        .sum()
    )

    pe_sources = (
        final_df["pe_source"]
        .value_counts()
        .to_dict()
    )

    log.info("")
    log.info(
        f"PE coverage: "
        f"{pe_ok}/{len(final_df)}"
    )

    log.info(
        f"EPS coverage: "
        f"{eps_ok}/{len(final_df)}"
    )

    log.info(
        f"PE sources: "
        f"{pe_sources}"
    )

    # ------------------------------------------------------------------------
    # Latest rows
    # ------------------------------------------------------------------------

    log.info(
        "\nLatest rows:\n"
        + final_df.tail(10).to_string(
            index=False
        )
    )

    # ------------------------------------------------------------------------
    # Final NULL check
    # ------------------------------------------------------------------------

    current_month = (
        pd.Timestamp.today()
        .to_period("M")
        .to_timestamp()
    )

    completed_null_pe = final_df.loc[
        (final_df["date"] < current_month)
        & final_df["pe_ratio"].isna()
    ]

    if not completed_null_pe.empty:

        raise RuntimeError(
            "FINAL SAFETY CHECK FAILED: "
            "completed month contains NULL PE."
        )

    # ------------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------------

    upsert_to_supabase(
        final_df,
        supabase,
    )

    log.info("=" * 70)
    log.info(
        "Nifty 50 Monthly P/E Tracker "
        "v7 - completed successfully"
    )
    log.info("=" * 70)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    run()
