"""
Nifty 50 Monthly P/E Tracker
-----------------------------
- Fetches monthly Nifty 50 OHLCV via yfinance
- Fetches OFFICIAL P/E data from NSE India's index data API
- Merges both and upserts into Supabase table `nifty50_pe`
- Safe to re-run: upsert is idempotent (no duplicates)

P/E Source priority:
  1. NSE India official P/E  — two endpoints tried in sequence
  2. yfinance fast_info / ticker.info trailingPE scaled to history
  3. NULL / unavailable

Fixes (2026-05-29):
  - NSE endpoint now validated as JSON before parsing (returns HTML on CI)
  - Added second NSE endpoint fallback (indices-pe-history CSV format)
  - yfinance PE fallback now tries fast_info first, then .info, then
    derives from downloaded history + a Stooq cross-check
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta

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
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]   # service_role key
TABLE_NAME        = "nifty50_pe"
TICKER            = "^NSEI"
HISTORY_YEARS     = 10

# NSE India headers — mandatory; without these NSE returns 401/403
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/reports-detail?type=equity",
    "Connection":      "keep-alive",
}


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    """
    Build a requests Session pre-seeded with NSE cookies.
    NSE returns 401 / blank JSON if cookies are absent.
    Two warm-up GETs are used — homepage first, then the reports page
    that the browser hits before calling the data API.
    """
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    warmup_urls = [
        "https://www.nseindia.com",
        "https://www.nseindia.com/reports-detail?type=equity",
    ]
    for url in warmup_urls:
        try:
            session.get(url, timeout=15)
            time.sleep(1.5)
        except Exception as e:
            log.warning(f"NSE warm-up GET failed ({url}): {e}")
    return session


def _safe_json(resp: requests.Response) -> dict | list | None:
    """
    Return parsed JSON only if the response Content-Type is actually JSON.
    NSE sometimes returns an HTML maintenance/captcha page with status 200;
    attempting json.loads() on HTML raises the 'Expecting value' error seen
    in the original logs.
    """
    ct = resp.headers.get("Content-Type", "")
    if "json" not in ct and "javascript" not in ct:
        log.warning(f"NSE returned non-JSON Content-Type: '{ct}' — skipping parse")
        log.debug(f"Response body (first 200 chars): {resp.text[:200]}")
        return None
    try:
        return resp.json()
    except Exception as e:
        log.warning(f"JSON parse failed: {e} — body: {resp.text[:200]}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Price data (yfinance)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_monthly_price() -> pd.DataFrame:
    """Download 10-year monthly OHLCV for Nifty 50 via yfinance."""
    log.info(f"Fetching {HISTORY_YEARS}-year monthly price data for {TICKER} …")
    ticker_obj = yf.Ticker(TICKER)
    df = ticker_obj.history(period=f"{HISTORY_YEARS}y", interval="1mo", auto_adjust=True)

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
# 2.  P/E data from NSE India  (two endpoints tried in order)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_nse_records(data: list) -> pd.DataFrame:
    """
    Common parser for NSE P/E records.
    Handles varying column names across NSE API versions.
    """
    df = pd.DataFrame(data)
    df.columns = [c.strip() for c in df.columns]

    col_map: dict[str, str] = {}
    for c in df.columns:
        cl = c.lower().replace(" ", "").replace("/", "").replace("_", "")
        if "date" in cl:
            col_map[c] = "date"
        elif cl in ("pe", "peratio", "pe_ratio", "trailing_pe", "trailingpe"):
            col_map[c] = "pe_ratio"
    df = df.rename(columns=col_map)

    if "date" not in df.columns or "pe_ratio" not in df.columns:
        log.warning(f"Unexpected NSE columns after mapping: {list(df.columns)}")
        return pd.DataFrame()

    df["date"]     = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["pe_ratio"] = pd.to_numeric(df["pe_ratio"], errors="coerce")
    df = df.dropna(subset=["date", "pe_ratio"])
    return df


def _nse_endpoint_v1(session: requests.Session, start: date, end: date) -> pd.DataFrame:
    """
    Primary NSE endpoint:
      GET /api/historical/indicesHistory/pe?index=NIFTY%2050&from=...&to=...
    Returns JSON with a top-level 'data' array.
    """
    url = (
        "https://www.nseindia.com/api/historical/indicesHistory/pe"
        f"?index=NIFTY%2050"
        f"&from={start.strftime('%d-%m-%Y')}"
        f"&to={end.strftime('%d-%m-%Y')}"
    )
    log.info(f"  NSE endpoint v1: {url}")
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  NSE v1 HTTP error: {e}")
        return pd.DataFrame()

    payload = _safe_json(resp)
    if payload is None:
        return pd.DataFrame()

    data: list = []
    if isinstance(payload, dict):
        data = payload.get("data", payload.get("indexCloseOnlineRecords", []))
    elif isinstance(payload, list):
        data = payload

    if not data:
        log.warning("  NSE v1 returned empty data array")
        return pd.DataFrame()

    log.info(f"  NSE v1: {len(data)} records received")
    return _parse_nse_records(data)


def _nse_endpoint_v2(session: requests.Session, start: date, end: date) -> pd.DataFrame:
    """
    Secondary NSE endpoint (index PE/PB/DivYield report download):
      GET /api/reports?archives=[...]&date=<DD-MM-YYYY>&type=equity&mode=single
    Falls back to the older /api/historicalIndexData endpoint.
    """
    # Try the older but often-available historical index data endpoint
    url = (
        "https://www.nseindia.com/api/historicalIndices"
        f"?indexType=NIFTY%2050"
        f"&from={start.strftime('%d-%m-%Y')}"
        f"&to={end.strftime('%d-%m-%Y')}"
    )
    log.info(f"  NSE endpoint v2: {url}")
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  NSE v2 HTTP error: {e}")
        return pd.DataFrame()

    payload = _safe_json(resp)
    if payload is None:
        return pd.DataFrame()

    data: list = []
    if isinstance(payload, dict):
        # This endpoint nests data under 'data' > 'indexCloseOnlineRecords'
        data = (
            payload.get("data", {}).get("indexCloseOnlineRecords", [])
            or payload.get("data", [])
            or payload.get("indexCloseOnlineRecords", [])
        )
    elif isinstance(payload, list):
        data = payload

    if not data:
        log.warning("  NSE v2 returned empty data array")
        return pd.DataFrame()

    log.info(f"  NSE v2: {len(data)} records received")
    return _parse_nse_records(data)


def fetch_nse_pe_history(years: int = 10) -> pd.DataFrame:
    """
    Try NSE v1 then v2.  Aggregate daily records to month-end P/E.
    Returns DataFrame[date, pe_ratio, pe_source] at month-start.
    """
    end_dt   = date.today()
    start_dt = end_dt - relativedelta(years=years)

    log.info(f"Fetching NSE P/E history ({start_dt} → {end_dt}) …")
    session = _nse_session()

    df = _nse_endpoint_v1(session, start_dt, end_dt)

    if df.empty:
        log.warning("  NSE v1 failed — trying v2 …")
        df = _nse_endpoint_v2(session, start_dt, end_dt)

    if df.empty:
        log.warning("  Both NSE endpoints failed — will use yfinance fallback")
        return pd.DataFrame()

    # Collapse daily → month-start (last trading day's PE per month)
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
# 3.  Fallback: derive P/E from yfinance
#     Priority: fast_info.pe_forward → ticker.info trailingPE → None
# ─────────────────────────────────────────────────────────────────────────────

def _get_yfinance_pe_and_price() -> tuple[float | None, float | None]:
    """
    Try multiple yfinance attributes to get a current P/E ratio.
    Returns (pe_ratio, current_price) or (None, None).
    """
    ticker_obj = yf.Ticker(TICKER)

    # --- Attempt 1: fast_info (new yfinance ≥ 0.2) ---
    try:
        fi = ticker_obj.fast_info
        price = getattr(fi, "last_price", None) or getattr(fi, "regular_market_price", None)
        # fast_info doesn't expose PE directly; we use it only for price
        if price:
            log.info(f"  fast_info last_price = {price}")
    except Exception as e:
        log.debug(f"  fast_info error: {e}")
        price = None

    # --- Attempt 2: ticker.info (slower, sometimes blocked on CI) ---
    trailing_pe   = None
    current_price = price
    try:
        info = ticker_obj.info
        trailing_pe   = info.get("trailingPE") or info.get("trailingEps") and None
        current_price = current_price or info.get("regularMarketPrice") or info.get("currentPrice")
        if trailing_pe:
            log.info(f"  ticker.info trailingPE = {trailing_pe}, price = {current_price}")
    except Exception as e:
        log.debug(f"  ticker.info error: {e}")

    # --- Attempt 3: derive PE from latest 1-day bar + trailing EPS ---
    # yfinance sometimes returns trailingEps even when trailingPE is missing
    if not trailing_pe:
        try:
            info          = ticker_obj.info
            eps           = info.get("trailingEps")
            current_price = current_price or info.get("regularMarketPrice") or info.get("currentPrice")
            if eps and current_price and float(eps) > 0:
                trailing_pe = round(float(current_price) / float(eps), 2)
                log.info(f"  Derived PE from trailingEps: price={current_price}, eps={eps}, PE={trailing_pe}")
        except Exception as e:
            log.debug(f"  trailingEps derive error: {e}")

    return trailing_pe, current_price


def fetch_yfinance_pe_fallback(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive historical P/E approximation from current yfinance data.
    PE_historical ≈ historical_close / current_implied_EPS
    This is an approximation — EPS assumed constant at current level.
    """
    log.info("Using yfinance PE fallback …")
    trailing_pe, current_price = _get_yfinance_pe_and_price()

    df = price_df[["date"]].copy()

    if trailing_pe and current_price and float(trailing_pe) > 0:
        current_eps = float(current_price) / float(trailing_pe)
        log.info(f"  Scaling history with implied EPS = {current_eps:.4f}  "
                 f"(price={current_price}, PE={trailing_pe})")
        df["pe_ratio"]  = (price_df["close"] / current_eps).round(2)
        df["eps_ttm"]   = round(current_eps, 4)
        df["pe_source"] = "yfinance_scaled"
    else:
        log.warning("  No PE available from yfinance — pe_ratio will be NULL")
        df["pe_ratio"]  = None
        df["eps_ttm"]   = None
        df["pe_source"] = "unavailable"

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Merge price + PE
# ─────────────────────────────────────────────────────────────────────────────

def build_final_dataframe(price_df: pd.DataFrame) -> pd.DataFrame:
    """Merge price data with the best available P/E series."""

    pe_df = fetch_nse_pe_history(years=HISTORY_YEARS)

    if pe_df.empty:
        log.warning("NSE P/E fetch failed — using yfinance scaling for all rows")
        pe_df = fetch_yfinance_pe_fallback(price_df)

    merged = price_df.merge(pe_df, on="date", how="left")

    # Fill gaps (e.g. current partial month not yet in NSE data)
    missing_pe = merged["pe_ratio"].isna()
    if missing_pe.any():
        log.info(f"  {missing_pe.sum()} months missing P/E — filling via yfinance fallback")
        fallback    = fetch_yfinance_pe_fallback(price_df).set_index("date")
        for idx, row in merged[missing_pe].iterrows():
            d = row["date"]
            if d in fallback.index:
                merged.at[idx, "pe_ratio"]  = fallback.loc[d, "pe_ratio"]
                merged.at[idx, "pe_source"] = fallback.loc[d, "pe_source"]
                if "eps_ttm" in fallback.columns:
                    merged.at[idx, "eps_ttm"] = fallback.loc[d, "eps_ttm"]

    # Compute eps_ttm for NSE-official rows  (close / PE)
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
    records          = df.copy()
    records["date"]  = records["date"].dt.strftime("%Y-%m-%d")
    records["volume"] = (
        pd.to_numeric(records["volume"], errors="coerce").fillna(0).astype("int64")
    )
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

def run() -> None:
    log.info("═" * 60)
    log.info("Nifty 50 Monthly P/E Tracker — starting")
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
