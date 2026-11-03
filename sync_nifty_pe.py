import os
import sys
import json
import time
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import yfinance as yf


# ============================================================
# CONFIGURATION
# ============================================================

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

DAILY_TABLE = "nifty50_pe_daily"
MONTHLY_TABLE = "nifty50_pe"
TICKER = "^NSEI"
INDEX_NAME = "NIFTY 50"

IST = ZoneInfo("Asia/Kolkata")

HISTORICAL_START = date(2016, 1, 1)
REQUEST_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT = 60

NIFTY_INDICES_BASE = "https://www.niftyindices.com"
HISTORICAL_PAGE = (
    "https://www.niftyindices.com/reports/historical-data"
)

PE_HISTORY_URL = (
    f"{NIFTY_INDICES_BASE}/Backpage.aspx/"
    "getpepbHistoricaldataDBtoString"
)

PRICE_HISTORY_URL = (
    f"{NIFTY_INDICES_BASE}/Backpage.aspx/"
    "getHistoricaldatatabletoString"
)

NSE_HOME_URL = "https://www.nseindia.com/"
NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"


# ============================================================
# LOGGING AND HTTP SESSION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("nifty50-pe")

session = requests.Session()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
})


# ============================================================
# GENERAL HELPERS
# ============================================================

def current_ist_date():
    return datetime.now(IST).date()


def to_float(value):
    if value is None:
        return None

    value = str(value).strip()

    if value in ("", "-", "NA", "N/A", "null", "None"):
        return None

    value = value.replace(",", "")

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    return result


def parse_nifty_date(value):
    """Parse dates returned by Nifty Indices."""
    if not value:
        raise ValueError("Historical record has no date")

    value = str(value).strip()

    formats = [
        "%d %b %Y",
        "%d-%b-%Y",
        "%d %B %Y",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    raise ValueError(f"Unrecognised date format: {value}")


def supabase_headers():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials are missing")

    return {
        "apikey": SUPABASE_KEY.strip(),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


# ============================================================
# SUPABASE OPERATIONS
# ============================================================

def supabase_upsert(table, rows, conflict="date,ticker"):
    if not rows:
        log.warning("No rows to upsert into %s", table)
        return

    url = f"{SUPABASE_URL}/rest/v1/{table}"

    headers = supabase_headers()
    params = {"on_conflict": conflict}

    batch_size = 500

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]

        response = requests.post(
            url,
            headers=headers,
            params=params,
            json=batch,
            timeout=REQUEST_TIMEOUT,
        )

        if not response.ok:
            raise RuntimeError(
                f"Supabase upsert failed for {table}: "
                f"{response.status_code} {response.text[:1500]}"
            )

    log.info(
        "Successfully upserted %s rows into %s",
        len(rows),
        table,
    )


def supabase_get_daily(start_date, end_date):
    """Read daily observations from Supabase with pagination."""

    url = f"{SUPABASE_URL}/rest/v1/{DAILY_TABLE}"

    headers = supabase_headers()

    output = []
    offset = 0
    page_size = 1000

    while True:
        params = [
            ("select", "date,ticker,close,pe_ratio,pb_ratio,div_yield,pe_source"),
            ("ticker", f"eq.{TICKER}"),
            ("date", f"gte.{start_date}"),
            ("date", f"lte.{end_date}"),
            ("order", "date.asc"),
            ("limit", str(page_size)),
            ("offset", str(offset)),
        ]

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if not response.ok:
            raise RuntimeError(
                f"Supabase read failed: "
                f"{response.status_code} {response.text[:1500]}"
            )

        page = response.json()

        if not isinstance(page, list):
            raise RuntimeError(
                f"Unexpected Supabase response: {str(page)[:1000]}"
            )

        output.extend(page)

        if len(page) < page_size:
            break

        offset += page_size

    return output


# ============================================================
# NIFTY INDICES HISTORICAL API
# ============================================================

def warm_nifty_indices_session():
    """
    Warm up the Nifty Indices session.

    The historical endpoint is undocumented and access behaviour
    can change. This GET is defensive; the POST response is still
    validated independently.
    """

    try:
        response = session.get(
            HISTORICAL_PAGE,
            timeout=15,
        )

        log.info(
            "Nifty Indices page status: %s",
            response.status_code,
        )

    except requests.RequestException as exc:
        log.warning(
            "Historical page warm-up failed; will try API directly: %s",
            exc,
        )


def historical_request_payload(start_date, end_date):
    """
    Nifty Indices expects cinfo to be a string containing a
    single-quoted object, not a nested JSON object.
    """

    start_text = start_date.strftime("%d-%b-%Y")
    end_text = end_date.strftime("%d-%b-%Y")

    cinfo = (
        "{"
        f"'name':'{INDEX_NAME}',"
        f"'startDate':'{start_text}',"
        f"'endDate':'{end_text}',"
        f"'indexName':'{INDEX_NAME}'"
        "}"
    )

    return {"cinfo": cinfo}


def post_historical_endpoint(url, start_date, end_date):
    payload = historical_request_payload(start_date, end_date)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": NIFTY_INDICES_BASE,
        "Referer": HISTORICAL_PAGE,
    }

    response = session.post(
        url,
        headers=headers,
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT,
    )

    if not response.ok:
        raise RuntimeError(
            f"Nifty Indices HTTP {response.status_code}: "
            f"{response.text[:1500]}"
        )

    try:
        outer = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Nifty Indices did not return JSON: "
            f"{response.text[:1000]}"
        ) from exc

    if not isinstance(outer, dict) or "d" not in outer:
        raise RuntimeError(
            f"Unexpected historical response structure: "
            f"{str(outer)[:1000]}"
        )

    raw_data = outer["d"]

    if isinstance(raw_data, str):
        try:
            records = json.loads(raw_data)
        except ValueError as exc:
            raise RuntimeError(
                f"Could not decode historical response field 'd': "
                f"{raw_data[:1000]}"
            ) from exc
    else:
        records = raw_data

    if not isinstance(records, list):
        raise RuntimeError(
            f"Historical response is not a list: "
            f"{str(records)[:1000]}"
        )

    return records


def fetch_historical_pe(start_date, end_date):
    records = post_historical_endpoint(
        PE_HISTORY_URL,
        start_date,
        end_date,
    )

    rows = []

    for item in records:
        observation_date = parse_nifty_date(
            item.get("DATE") or item.get("Date")
        )

        pe = to_float(item.get("pe"))
        pb = to_float(item.get("pb"))
        div_yield = to_float(item.get("divYield"))

        rows.append({
            "date": observation_date.isoformat(),
            "ticker": TICKER,
            "close": None,
            "pe_ratio": pe,
            "pb_ratio": pb,
            "div_yield": div_yield,
            "pe_source": "Nifty Indices historical data",
        })

    return rows


def fetch_historical_closes(start_date, end_date):
    records = post_historical_endpoint(
        PRICE_HISTORY_URL,
        start_date,
        end_date,
    )

    closes = {}

    for item in records:
        observation_date = parse_nifty_date(
            item.get("HistoricalDate")
            or item.get("DATE")
            or item.get("Date")
        )

        close = to_float(item.get("CLOSE"))

        if close is not None:
            closes[observation_date.isoformat()] = close

    return closes


def fetch_historical_chunk(start_date, end_date):
    """
    Fetch historical P/E and index closing values for one chunk.
    """

    log.info(
        "Fetching historical data: %s to %s",
        start_date,
        end_date,
    )

    pe_rows = fetch_historical_pe(start_date, end_date)

    time.sleep(REQUEST_DELAY_SECONDS)

    close_map = fetch_historical_closes(start_date, end_date)

    for row in pe_rows:
        row["close"] = close_map.get(row["date"])

    log.info(
        "Historical chunk: %s PE records, %s closing prices",
        len(pe_rows),
        len(close_map),
    )

    return pe_rows


# ============================================================
# HISTORICAL BACKFILL
# ============================================================

def run_backfill():
    """
    Fetch daily PE and close history from Nifty Indices.

    Uses annual chunks to reduce the chance of server rejection.
    Existing rows are updated using the date,ticker primary key.
    """

    warm_nifty_indices_session()

    end_date = current_ist_date()
    chunk_start = HISTORICAL_START

    all_rows = []

    while chunk_start <= end_date:
        chunk_end = min(
            chunk_start + timedelta(days=364),
            end_date,
        )

        rows = fetch_historical_chunk(
            chunk_start,
            chunk_end,
        )

        all_rows.extend(rows)

        chunk_start = chunk_end + timedelta(days=1)

        time.sleep(REQUEST_DELAY_SECONDS)

    if not all_rows:
        raise RuntimeError(
            "Historical backfill returned zero rows. "
            "No database update performed."
        )

    # Ensure duplicate dates within the downloaded data are removed.
    deduplicated = {}

    for row in all_rows:
        key = (row["date"], row["ticker"])
        deduplicated[key] = row

    final_rows = list(deduplicated.values())

    final_rows.sort(key=lambda x: x["date"])

    pe_count = sum(
        1 for row in final_rows
        if row["pe_ratio"] is not None
    )

    if pe_count == 0:
        raise RuntimeError(
            "Historical response contained no valid PE values."
        )

    supabase_upsert(DAILY_TABLE, final_rows)

    log.info(
        "Backfill complete: %s rows, %s with PE values, %s to %s",
        len(final_rows),
        pe_count,
        final_rows[0]["date"],
        final_rows[-1]["date"],
    )


# ============================================================
# LIVE NSE DAILY COLLECTION
# ============================================================

def parse_nse_observation_date(record):
    """
    Parse the observation date from NSE's index record.

    If the response has no parseable timestamp, use today's India
    date only as a fallback. The caller logs this fallback.
    """

    candidates = [
        record.get("lastUpdateTime"),
        record.get("timestamp"),
        record.get("time"),
    ]

    formats = [
        "%d-%b-%Y %H:%M:%S",
        "%d-%b-%Y",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y",
        "%d-%m-%Y %H:%M:%S",
    ]

    for candidate in candidates:
        if not candidate:
            continue

        for fmt in formats:
            try:
                return datetime.strptime(
                    str(candidate).strip(),
                    fmt,
                ).date()
            except ValueError:
                continue

    log.warning(
        "NSE record did not contain a recognised date. "
        "Using current India date as fallback."
    )

    return current_ist_date()


def fetch_nse_live_pe():
    """
    Fetch the latest NIFTY 50 P/E snapshot from NSE.

    This endpoint provides the current snapshot; it is not a
    historical daily series.
    """

    log.info("Warming up NSE session")

    home_response = session.get(
        NSE_HOME_URL,
        timeout=30,
    )
    home_response.raise_for_status()

    time.sleep(1)

    response = session.get(
        NSE_ALL_INDICES_URL,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()

    records = payload.get("data", [])

    record = next(
        (
            item for item in records
            if str(item.get("index", "")).strip().upper()
            == INDEX_NAME
        ),
        None,
    )

    if record is None:
        raise RuntimeError(
            "NIFTY 50 was not found in NSE allIndices response."
        )

    pe = to_float(record.get("pe"))
    close = to_float(record.get("last"))

    if pe is None or pe <= 0:
        raise RuntimeError(
            f"NSE returned invalid NIFTY 50 PE: {record.get('pe')}"
        )

    observation_date = parse_nse_observation_date(record)

    if observation_date > current_ist_date():
        raise RuntimeError(
            f"NSE returned a future observation date: {observation_date}"
        )

    return {
        "date": observation_date.isoformat(),
        "ticker": TICKER,
        "close": close,
        "pe_ratio": pe,
        "pb_ratio": to_float(record.get("pb")),
        "div_yield": to_float(record.get("divYield")),
        "pe_source": "NSE allIndices live snapshot",
    }


def run_daily():
    """
    Fetch the latest published Nifty 50 daily PE from
    Nifty Indices historical data.

    Does not access nseindia.com.
    """

    log.info("Starting daily Nifty 50 PE collection")
    warm_nifty_indices_session()

    today = current_ist_date()
    start_date = today - timedelta(days=15)

    log.info(
        "Requesting recent Nifty PE history: %s to %s",
        start_date,
        today,
    )

    # Fetch recent daily PE records.
    rows = fetch_historical_pe(start_date, today)

    if not rows:
        raise RuntimeError(
            "Nifty Indices returned no recent PE observations."
        )

    # Keep only valid observations up to today's date.
    valid_rows = [
        row for row in rows
        if (
            row.get("pe_ratio") is not None
            and row.get("date")
            and row["date"] <= today.isoformat()
        )
    ]

    if not valid_rows:
        raise RuntimeError(
            "No valid recent Nifty PE observations were returned."
        )

    # Select the latest available trading-day observation.
    latest = max(valid_rows, key=lambda x: x["date"])

    # Fetch the corresponding index closing price.
    latest_date = datetime.strptime(
        latest["date"], "%Y-%m-%d"
    ).date()

    close_map = fetch_historical_closes(
        latest_date,
        latest_date,
    )

    latest["close"] = close_map.get(latest["date"])
    latest["pe_source"] = "Nifty Indices daily historical data"

    log.info(
        "Latest available observation: date=%s PE=%s close=%s",
        latest["date"],
        latest["pe_ratio"],
        latest["close"],
    )

    # Save the daily observation.
    supabase_upsert(
        DAILY_TABLE,
        [latest],
    )

    log.info("Daily PE observation saved successfully.")

    # Rebuild monthly history.
    rebuild_monthly()

    log.info("Daily collection completed successfully.")


# ============================================================
# MONTHLY AGGREGATION
# ============================================================

def fetch_monthly_closes():
    """
    Download monthly Nifty 50 closing prices from Yahoo Finance.

    Yahoo Finance is used only for monthly index close values.
    P/E values always come from the daily NSE/Nifty Indices data.
    """

    today = current_ist_date()

    data = yf.download(
        TICKER,
        start=HISTORICAL_START.isoformat(),
        end=(today + timedelta(days=2)).isoformat(),
        interval="1mo",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if data.empty:
        raise RuntimeError(
            "Yahoo Finance returned no monthly Nifty price data."
        )

    if isinstance(data.columns, pd.MultiIndex):
        close_series = data["Close"].iloc[:, 0]
    else:
        close_series = data["Close"]

    monthly_closes = {}

    for timestamp, value in close_series.dropna().items():
        month_key = pd.Timestamp(timestamp).strftime("%Y-%m")
        close = to_float(value)

        if close is not None:
            monthly_closes[month_key] = close

    return monthly_closes


def rebuild_monthly():
    """
    Build the monthly table from the last available daily PE
    observation in each calendar month.

    Monthly date convention: first day of each month.
    """

    log.info("Rebuilding monthly Nifty PE table")

    today = current_ist_date()

    daily_rows = supabase_get_daily(
        HISTORICAL_START.isoformat(),
        today.isoformat(),
    )

    if not daily_rows:
        raise RuntimeError(
            "No daily observations found in Supabase. "
            "Run backfill before rebuilding monthly data."
        )

    # Keep only observations with a valid PE.
    valid_rows = [
        row for row in daily_rows
        if to_float(row.get("pe_ratio")) is not None
    ]

    if not valid_rows:
        raise RuntimeError(
            "No valid daily PE observations found."
        )

    # Select the last PE observation in each calendar month.
    latest_by_month = {}

    for row in valid_rows:
        month_key = row["date"][:7]

        existing = latest_by_month.get(month_key)

        if existing is None or row["date"] > existing["date"]:
            latest_by_month[month_key] = row

    monthly_closes = fetch_monthly_closes()

    monthly_rows = []

    for month_key in sorted(latest_by_month):
        row = latest_by_month[month_key]

        pe = to_float(row.get("pe_ratio"))

        # Prefer the close associated with the selected PE date.
        # Historical PE endpoint close is populated during backfill.
        close = to_float(row.get("close"))

        # If daily close is unavailable, use Yahoo monthly close.
        if close is None:
            close = monthly_closes.get(month_key)

        eps_ttm = None

        if close is not None and pe is not None and pe > 0:
            eps_ttm = round(close / pe, 2)

        monthly_rows.append({
            "date": f"{month_key}-01",
            "ticker": TICKER,
            "close": close,
            "pe": pe,
            "eps_ttm": eps_ttm,
        })

    if not monthly_rows:
        raise RuntimeError(
            "Monthly aggregation produced zero rows."
        )

    supabase_upsert(
        MONTHLY_TABLE,
        monthly_rows,
    )

    log.info(
        "Monthly table refreshed: %s records (%s to %s)",
        len(monthly_rows),
        monthly_rows[0]["date"],
        monthly_rows[-1]["date"],
    )


# ============================================================
# MAIN
# ============================================================

def main():
    mode = (
        sys.argv[1].lower()
        if len(sys.argv) > 1
        else "daily"
    )

    log.info("=" * 60)
    log.info("NIFTY 50 DAILY P/E TRACKER")
    log.info("Mode: %s", mode)
    log.info("=" * 60)

    if mode == "daily":
        run_daily()

    elif mode == "backfill":
        run_backfill()
        rebuild_monthly()

    elif mode == "rebuild":
        rebuild_monthly()

    else:
        raise SystemExit(
            "Invalid mode. Use: daily, backfill, or rebuild."
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Nifty PE tracker failed")
        raise
