# Nifty 50 Monthly P/E Tracker

Automatically fetches **10 years of Nifty 50 monthly P/E data** via `yfinance`,
stores it in **Supabase (PostgreSQL)**, and **auto-updates every month** via GitHub Actions.

---

## 📁 Project Structure

```
nifty50-pe-tracker/
├── fetch_nifty_pe.py              # Main fetch + upsert script
├── supabase_migration.sql         # One-time DB table setup
├── requirements.txt
├── .github/
│   └── workflows/
│       └── monthly_pe_update.yml  # GitHub Actions schedule
└── README.md
```

---

## 🚀 Setup (One-Time)

### Step 1 — Create the Supabase Table

1. Go to your [Supabase project](https://supabase.com) → **SQL Editor**
2. Paste and run the contents of `supabase_migration.sql`
3. This creates the `nifty50_pe` table, indexes, RLS policies, and helper views

### Step 2 — Get Supabase Credentials

From your Supabase project dashboard:
- **URL**: Settings → API → Project URL
- **Key**: Settings → API → `service_role` secret key *(not the anon key)*

### Step 3 — Add GitHub Secrets

In your GitHub repo → **Settings → Secrets and variables → Actions**, add:

| Secret Name    | Value                          |
|----------------|-------------------------------|
| `SUPABASE_URL` | `https://xxxx.supabase.co`    |
| `SUPABASE_KEY` | `service_role` key from above |

### Step 4 — Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit: Nifty 50 P/E tracker"
git remote add origin https://github.com/YOUR_USER/nifty50-pe-tracker.git
git push -u origin main
```

GitHub Actions will automatically trigger on the **1st of every month at 06:00 UTC (11:30 IST)**.

---

## 🔄 Manual Run (Backfill / Debug)

```bash
# Local run
export SUPABASE_URL="https://xxxx.supabase.co"
export SUPABASE_KEY="your-service-role-key"
pip install -r requirements.txt
python fetch_nifty_pe.py
```

Or trigger manually from GitHub:
**Actions → Nifty 50 Monthly P/E Update → Run workflow**

---

## 🗄️ Table Schema: `nifty50_pe`

| Column       | Type          | Description                                 |
|--------------|---------------|---------------------------------------------|
| `id`         | BIGSERIAL     | Auto-increment PK                           |
| `date`       | DATE          | Month-start date (e.g. `2024-01-01`)        |
| `ticker`     | TEXT          | `^NSEI`                                     |
| `open`       | NUMERIC(12,2) | Monthly open price                          |
| `high`       | NUMERIC(12,2) | Monthly high price                          |
| `low`        | NUMERIC(12,2) | Monthly low price                           |
| `close`      | NUMERIC(12,2) | Monthly close price                         |
| `volume`     | BIGINT        | Total monthly volume                        |
| `pe_ratio`   | NUMERIC(8,2)  | Trailing P/E ratio                          |
| `eps_ttm`    | NUMERIC(10,4) | TTM EPS used in calculation                 |
| `pe_source`  | TEXT          | `yfinance_scaled` / `actual` / `unavailable`|
| `updated_at` | TIMESTAMPTZ   | Last upsert timestamp                       |

**Primary uniqueness**: `(date, ticker)` — safe to re-run, no duplicates.

---

## 📊 Useful SQL Queries

```sql
-- Last 12 months P/E
SELECT date, close, pe_ratio
FROM nifty50_pe
ORDER BY date DESC
LIMIT 12;

-- Year-over-year comparison
SELECT * FROM nifty50_pe_yoy LIMIT 24;

-- P/E percentile (how expensive vs history)
SELECT
    date, pe_ratio,
    PERCENT_RANK() OVER (ORDER BY pe_ratio) * 100 AS pe_percentile
FROM nifty50_pe
ORDER BY date DESC;

-- Average P/E by year
SELECT
    EXTRACT(YEAR FROM date) AS year,
    ROUND(AVG(pe_ratio), 2) AS avg_pe,
    ROUND(MIN(pe_ratio), 2) AS min_pe,
    ROUND(MAX(pe_ratio), 2) AS max_pe
FROM nifty50_pe
GROUP BY 1
ORDER BY 1 DESC;
```

---

## ⚠️ P/E Calculation Note

Yahoo Finance (`yfinance`) provides a **current trailing P/E** snapshot via `ticker.info`.
For historical monthly P/E, this tool derives an **implied EPS** from the current price and
P/E, then applies it to all historical closes:

```
historical_PE  =  historical_close  /  current_implied_EPS
```

This is a **reasonable approximation** — older values may be slightly overstated because
EPS grows over time. For precise historical P/E, you can replace `eps_ttm` column values
with actual NSE-published EPS data (available from NSE India's index data archives).

---

## 🔔 Alerts

If the GitHub Action fails (network error, API change, etc.), it will **automatically open
a GitHub Issue** in your repo with a link to the failed run.
