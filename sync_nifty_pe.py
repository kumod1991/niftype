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

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

DAILY_TABLE = "nifty50_pe_daily"
MONTHLY_TABLE = "nifty50_pe"

TICKER = "^NSEI"
INDEX_NAME = "NIFTY 50"

IST = ZoneInfo("Asia/Kolkata")

HISTORICAL_START = date(2016, 1, 1)

REQUEST_TIMEOUT = 60
REQUEST_DELAY = 1.0
BATCH_SIZE = 500

# Current Nifty Indices API route first; legacy route as fallback.
NIFTY_PE_ENDPOINTS = [
    "https://niftyindices.com/BackPage/getpepbHistoricaldataDBtoString",
    "https://www.niftyindices.com/BackPage/getpepbHistoricaldataDBtoString",
    "https://www.niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString",
]

NIFTY_PRICE_ENDPOINTS = [
    "https://niftyindices.com/BackPage/getHistoricaldatatabletoString",
    "https://www.niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString",
]

NIFTY_HISTORY_PAGE = (
    "https://www.niftyindices.com/reports/historical-data"
)

NIFTY_HOME = "https://niftyindices.com/"

# Yahoo Finance is used only as a fallback for monthly closing prices.
YAHOO_TICKER = "^NSEI"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("nifty50-pe")


# ============================================================
# HTTP SESSION
# ============================================================

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
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": "https://niftyindices.com",
    "Referer": NIFTY_HISTORY_PAGE,
    "X-Requested-With": "XMLHttpRequest",
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

    if value.lower() in (
        "", "-", "na", "n/a", "null", "none", "nan", "false"
    ):
        return None

    value = value.replace(",", "")

    try:
        result = float(value)
    except (ValueError, TypeError):
        return None

    if pd.isna(result):
        return None

    return result


def parse_nifty_date(value):
    if not value:
        raise ValueError("Missing date in Nifty Indices response")

    value = str(value).strip()

    formats = [
        "%d %b %Y",
        "%d-%b-%Y",
        "%d %B %Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    raise ValueError(
        f"Unrecognised Nifty Indices date: {value}"
    )


def date_range_chunks(start_date, end_date, days=364):
    current = start_date

    while current <= end_date:
        chunk_end = min(
            current + timedelta(days=days),
            end_date,
        )

        yield current, chunk_end

        current = chunk_end + timedelta(days=1)


# ============================================================
# SUPABASE AUTHENTICATION
# ============================================================

def validate_supabase_credentials():
    if not SUPABASE_URL:
        raise RuntimeError(
            "GitHub secret SUPABASE_URL is missing."
        )

    if not SUPABASE_KEY:
        raise RuntimeError(
            "GitHub secret SUPABASE_SERVICE_ROLE_KEY is missing."
        )


def supabase_headers():
    validate_supabase_credentials()

    # Supabase sb_secret_* API keys are sent in the apikey header.
    # Do not treat this key as a legacy JWT.
    return {
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


# ============================================================
# SUPABASE UPSERT
# ============================================================

def supabase_upsert(table, rows, conflict="date,ticker"):
    if not rows:
        log.warning("No rows supplied for table %s", table)
        return

    url = f"{SUPABASE_URL}/rest/v1/{table}"

    headers = supabase_headers()
    params = {"on_conflict": conflict}

    total = len(rows)

    for start in range(0, total, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]

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
                f"HTTP {response.status_code}: "
                f"{response.text[:2000]}"
            )

        log.info(
            "Upserted batch %s-%s of %s into %s",
            start + 1,
            start + len(batch),
            total,
            table,
        )

    log.info(
        "Successfully upserted %s rows into %s",
        total,
        table,
    )


# ============================================================
# SUPABASE READ
# ============================================================

def supabase_get_daily(start_date, end_date):
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
                f"Supabase daily read failed: "
                f"HTTP {response.status_code}: "
                f"{response.text[:2000]}"
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
# NIFTY INDICES SESSION
# ============================================================

def warm_nifty_indices_session():
    """
    Warm up the Nifty Indices website session.

    This does not access nseindia.com.
    """

    try:
        response = session.get(
            NIFTY_HOME,
            timeout=20,
        )

        log.info(
            "Nifty Indices home response: HTTP %s",
            response.status_code,
        )

    except requests.RequestException as exc:
        log.warning(
            "Nifty Indices home warm-up failed: %s",
            exc,
        )

    try:
        response = session.get(
            NIFTY_HISTORY_PAGE,
            timeout=20,
        )

        log.info(
            "Nifty Indices historical page response: HTTP %s",
            response.status_code,
        )

    except requests.RequestException as exc:
        log.warning(
            "Historical page warm-up failed: %s",
            exc,
        )


# ============================================================
# NIFTY INDICES REQUEST HELPERS
# ============================================================

def build_cinfo(start_date, end_date):
    """
    Build the request string expected by the Nifty Indices
    historical-data endpoints.
    """

    start_text = start_date.strftime("%d-%b-%Y")
    end_text = end_date.strftime("%d-%b-%Y")

    return (
        "{"
        f"'name':'{INDEX_NAME}',"
        f"'startDate':'{start_text}',"
        f"'endDate':'{end_text}',"
        f"'indexName':'{INDEX_NAME}'"
        "}"
    )


def parse_nifty_response(response):
    """
    Support both known response formats:

    Older:
        {"d": "[{...},{...}]"}

    Newer:
        [{...},{...}]

    Also handles a JSON-encoded string containing an array.
    """

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Nifty Indices response was not valid JSON: "
            + response.text[:1000]
        ) from exc

    if isinstance(payload, dict) and "d" in payload:
        payload = payload["d"]

    if isinstance(payload, str):
        stripped = payload.strip()

        if stripped.lower() in ("", "false", "[]"):
            return []

        try:
            payload = json.loads(stripped)
        except ValueError as exc:
            raise RuntimeError(
                "Could not decode Nifty Indices response data: "
                + stripped[:1000]
            ) from exc

    if payload is None or payload is False:
        return []

    if not isinstance(payload, list):
        raise RuntimeError(
            "Unexpected Nifty Indices response structure: "
            + str(payload)[:1500]
        )

    return payload


def call_nifty_endpoint(endpoints, start_date, end_date):
    """
    Try current Nifty Indices endpoint routes first, then legacy routes.
    """

    payload = {
        "cinfo": build_cinfo(start_date, end_date)
    }

    errors = []

    for url in endpoints:
        try:
            log.info("Requesting Nifty Indices endpoint: %s", url)

            response = session.post(
                url,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )

            if not response.ok:
                raise RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:1000]}"
                )

            records = parse_nifty_response(response)

            # Empty result can mean no trading days, no data,
            # or a rejected index/date request. Try next route.
            if not records:
                log.warning(
                    "Endpoint returned no records: %s",
                    url,
                )
                continue

            log.info(
                "Endpoint returned %s records",
                len(records),
            )

            return records

        except Exception as exc:
            message = f"{url}: {exc}"
            errors.append(message)

            log.warning(
                "Endpoint attempt failed: %s",
                message,
            )

        time.sleep(REQUEST_DELAY)

    raise RuntimeError(
        "All Nifty Indices endpoint attempts failed or returned "
        "no records.\n" + "\n".join(errors)
    )


# ============================================================
# HISTORICAL DAILY P/E
# ============================================================

def fetch_historical_pe(start_date, end_date):
    records = call_nifty_endpoint(
        NIFTY_PE_ENDPOINTS,
        start_date,
        end_date,
    )

    rows = []

    for item in records:
        raw_date = (
            item.get("DATE")
            or item.get("Date")
            or item.get("date")
        )

        if not raw_date:
            log.warning(
                "Skipping PE record without date: %s",
                str(item)[:300],
            )
            continue

        observation_date = parse_nifty_date(raw_date)

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

    rows.sort(key=lambda x: x["date"])

    return rows


# ============================================================
# HISTORICAL DAILY INDEX CLOSE
# ============================================================

def fetch_historical_closes(start_date, end_date):
    """
    Retrieve daily index closing values.

    If this secondary endpoint is unavailable, P/E collection
    can still proceed. Monthly close can be obtained from Yahoo.
    """

    records = call_nifty_endpoint(
        NIFTY_PRICE_ENDPOINTS,
        start_date,
        end_date,
    )

    closes = {}

    for item in records:
        raw_date = (
            item.get("HistoricalDate")
            or item.get("DATE")
            or item.get("Date")
        )

        if not raw_date:
            continue

        observation_date = parse_nifty_date(raw_date)
        close = to_float(item.get("CLOSE"))

        if close is not None:
            closes[observation_date.isoformat()] = close

    return closes


def attach_closes(pe_rows, start_date, end_date):
    """
    Attach closing values to P/E rows.

    Failure to obtain prices does not discard valid P/E records.
    """

    try:
        close_map = fetch_historical_closes(
            start_date,
            end_date,
        )

        for row in pe_rows:
            row["close"] = close_map.get(row["date"])

        log.info(
            "Attached closing prices to %s observations",
            sum(1 for row in pe_rows if row["close"] is not None),
        )

    except Exception as exc:
        log.warning(
            "Could not retrieve historical closing prices: %s",
            exc,
        )

    return pe_rows


# ============================================================
# HISTORICAL BACKFILL
# ============================================================

def run_backfill():
    log.info("=" * 60)
    log.info("STARTING HISTORICAL DAILY P/E BACKFILL")
    log.info("=" * 60)

    warm_nifty_indices_session()

    end_date = current_ist_date()

    all_rows = []

    for chunk_start, chunk_end in date_range_chunks(
        HISTORICAL_START,
        end_date,
        days=350,
    ):
        log.info(
            "Backfill period: %s to %s",
            chunk_start,
            chunk_end,
        )

        pe_rows = fetch_historical_pe(
            chunk_start,
            chunk_end,
        )

        if pe_rows:
            pe_rows = attach_closes(
                pe_rows,
                chunk_start,
                chunk_end,
            )

            all_rows.extend(pe_rows)

            log.info(
                "Collected %s P/E rows for this period",
                len(pe_rows),
            )
        else:
            log.warning(
                "No P/E observations for %s to %s",
                chunk_start,
                chunk_end,
            )

        time.sleep(REQUEST_DELAY)

    if not all_rows:
        raise RuntimeError(
            "Historical backfill returned zero P/E records. "
            "No Supabase data was changed."
        )

    # Deduplicate by date and ticker.
    deduplicated = {}

    for row in all_rows:
        key = (row["date"], row["ticker"])
        deduplicated[key] = row

    final_rows = sorted(
        deduplicated.values(),
        key=lambda x: x["date"],
    )

    valid_pe_count = sum(
        1 for row in final_rows
        if row["pe_ratio"] is not None
    )

    if valid_pe_count == 0:
        raise RuntimeError(
            "Backfill contained no valid P/E values."
        )

    supabase_upsert(
        DAILY_TABLE,
        final_rows,
    )

    log.info(
        "Historical backfill completed: %s total rows, "
        "%s valid P/E rows, period %s to %s",
        len(final_rows),
        valid_pe_count,
        final_rows[0]["date"],
        final_rows[-1]["date"],
    )


# ============================================================
# DAILY COLLECTION
# ============================================================

def run_daily():
    """
    Fetch the latest available published daily Nifty P/E.

    No NSE live API is called in this function.
    """

    log.info("=" * 60)
    log.info("STARTING DAILY NIFTY 50 P/E COLLECTION")
    log.info("Source: Nifty Indices historical data")
    log.info("=" * 60)

    warm_nifty_indices_session()

    today = current_ist_date()
    start_date = today - timedelta(days=15)

    log.info(
        "Fetching recent P/E observations: %s to %s",
        start_date,
        today,
    )

    rows = fetch_historical_pe(
        start_date,
        today,
    )

    if not rows:
        raise RuntimeError(
            "Nifty Indices returned no recent P/E observations."
        )

    valid_rows = [
        row for row in rows
        if (
            row["pe_ratio"] is not None
            and row["date"] <= today.isoformat()
        )
    ]

    if not valid_rows:
        raise RuntimeError(
            "No valid P/E observations were found in the response."
        )

    latest = max(
        valid_rows,
        key=lambda row: row["date"],
    )

    latest_date = datetime.strptime(
        latest["date"],
        "%Y-%m-%d",
    ).date()

    latest = attach_closes(
        [latest],
        latest_date,
        latest_date,
    )[0]

    latest["pe_source"] = "Nifty Indices historical data"

    log.info(
        "Latest available observation: date=%s | PE=%s | close=%s",
        latest["date"],
        latest["pe_ratio"],
        latest["close"],
    )

    supabase_upsert(
        DAILY_TABLE,
        [latest],
    )

    log.info("Daily P/E successfully saved to Supabase.")

    rebuild_monthly()

    log.info("Daily collection completed successfully.")


# ============================================================
# MONTHLY CLOSE PRICE FALLBACK
# ============================================================

def fetch_monthly_closes():
    today = current_ist_date()

    log.info("Fetching monthly Nifty 50 closing prices from Yahoo Finance")

    data = yf.download(
        YAHOO_TICKER,
        start=HISTORICAL_START.isoformat(),
        end=(today + timedelta(days=2)).isoformat(),
        interval="1mo",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if data.empty:
        log.warning(
            "Yahoo Finance returned no monthly prices."
        )
        return {}

    if isinstance(data.columns, pd.MultiIndex):
        close_series = data["Close"].iloc[:, 0]
    else:
        close_series = data["Close"]

    result = {}

    for timestamp, value in close_series.dropna().items():
        month_key = pd.Timestamp(timestamp).strftime("%Y-%m")
        close = to_float(value)

        if close is not None:
            result[month_key] = close

    log.info(
        "Downloaded %s monthly closing prices",
        len(result),
    )

    return result


# ============================================================
# MONTHLY AGGREGATION
# ============================================================

def rebuild_monthly():
    """
    Derive monthly Nifty P/E using the last available daily
    P/E observation of each calendar month.

    The monthly date key remains YYYY-MM-01 to preserve
    compatibility with the existing monthly table.
    """

    log.info("=" * 60)
    log.info("REBUILDING MONTHLY NIFTY 50 P/E TABLE")
    log.info("=" * 60)

    today = current_ist_date()

    daily_rows = supabase_get_daily(
        HISTORICAL_START.isoformat(),
        today.isoformat(),
    )

    if not daily_rows:
        raise RuntimeError(
            "No daily observations found in Supabase. "
            "Run backfill first."
        )

    valid_rows = []

    for row in daily_rows:
        pe = to_float(row.get("pe_ratio"))

        if pe is None or pe <= 0:
            continue

        valid_rows.append(row)

    if not valid_rows:
        raise RuntimeError(
            "No valid daily P/E observations found in Supabase."
        )

    latest_by_month = {}

    for row in valid_rows:
        month_key = row["date"][:7]

        existing = latest_by_month.get(month_key)

        if (
            existing is None
            or row["date"] > existing["date"]
        ):
            latest_by_month[month_key] = row

    monthly_closes = fetch_monthly_closes()

    monthly_rows = []

    for month_key in sorted(latest_by_month):
        row = latest_by_month[month_key]

        pe = to_float(row.get("pe_ratio"))
        close = to_float(row.get("close"))

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
            "Monthly aggregation generated zero records."
        )

    supabase_upsert(
        MONTHLY_TABLE,
        monthly_rows,
    )

    log.info(
        "Monthly table successfully refreshed: %s rows",
        len(monthly_rows),
    )

    log.info(
        "Monthly coverage: %s to %s",
        monthly_rows[0]["date"],
        monthly_rows[-1]["date"],
    )


# ============================================================
# MAIN
# ============================================================

def main():
    mode = (
        sys.argv[1].strip().lower()
        if len(sys.argv) > 1
        else "daily"
    )

    log.info("=" * 65)
    log.info("NIFTY 50 DAILY P/E TRACKER")
    log.info("Mode: %s", mode)
    log.info("=" * 65)

    validate_supabase_credentials()

    if mode == "daily":
        run_daily()

    elif mode == "backfill":
        run_backfill()
        rebuild_monthly()

    elif mode == "rebuild":
        rebuild_monthly()

    else:
        raise SystemExit(
            "Invalid mode. Use daily, backfill, or rebuild."
        )


if __name__ == "__main__":
    try:
        main()

    except Exception:
        log.exception("Nifty PE tracker failed")
        raise
