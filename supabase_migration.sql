-- ============================================================
-- Supabase Migration: Create nifty50_pe table
-- Run this once in the Supabase SQL Editor or via CLI
-- ============================================================

CREATE TABLE IF NOT EXISTS public.nifty50_pe (
    id          BIGSERIAL       PRIMARY KEY,
    date        DATE            NOT NULL,           -- Month-start (YYYY-MM-01)
    ticker      TEXT            NOT NULL DEFAULT '^NSEI',
    open        NUMERIC(12, 2),
    high        NUMERIC(12, 2),
    low         NUMERIC(12, 2),
    close       NUMERIC(12, 2)  NOT NULL,
    volume      BIGINT,
    pe_ratio    NUMERIC(8, 2),                      -- Trailing P/E ratio
    eps_ttm     NUMERIC(10, 4),                     -- Implied TTM EPS used for calculation
    pe_source   TEXT,                               -- 'yfinance_scaled' | 'actual' | 'unavailable'
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Composite unique key: one row per month per ticker
    CONSTRAINT uq_nifty50_pe_date_ticker UNIQUE (date, ticker)
);

-- Index for fast range queries (e.g. last 12 months, year-over-year)
CREATE INDEX IF NOT EXISTS idx_nifty50_pe_date
    ON public.nifty50_pe (date DESC);

CREATE INDEX IF NOT EXISTS idx_nifty50_pe_ticker_date
    ON public.nifty50_pe (ticker, date DESC);

-- Optional: enable Row Level Security (RLS) for read-only public access
ALTER TABLE public.nifty50_pe ENABLE ROW LEVEL SECURITY;

-- Allow anyone to read (adjust as needed)
CREATE POLICY "Public read" ON public.nifty50_pe
    FOR SELECT USING (true);

-- Only service_role (your GitHub Action) can insert/update
-- (No INSERT policy needed when using service_role key — it bypasses RLS)

-- ── Helpful views ────────────────────────────────────────────────────────────

-- Latest P/E snapshot
CREATE OR REPLACE VIEW public.nifty50_pe_latest AS
SELECT *
FROM public.nifty50_pe
ORDER BY date DESC
LIMIT 1;

-- Year-over-year P/E comparison
CREATE OR REPLACE VIEW public.nifty50_pe_yoy AS
SELECT
    a.date,
    a.close         AS price,
    a.pe_ratio      AS pe_current,
    b.pe_ratio      AS pe_1y_ago,
    ROUND(((a.pe_ratio - b.pe_ratio) / NULLIF(b.pe_ratio, 0)) * 100, 2) AS pe_change_pct
FROM public.nifty50_pe a
LEFT JOIN public.nifty50_pe b
    ON b.date = (a.date - INTERVAL '1 year')::DATE
   AND b.ticker = a.ticker
WHERE a.ticker = '^NSEI'
ORDER BY a.date DESC;
