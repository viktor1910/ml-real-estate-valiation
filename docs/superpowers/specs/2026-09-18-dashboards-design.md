# Dashboards + Cloudflare Tunnel — Design Spec

**Date:** 2026-09-18
**Status:** Approved for planning

## Goal

Two web dashboards over one Streamlit app, exposed for demo via a Cloudflare
quick tunnel:

1. **Report** — view scored predictions (read-only).
2. **CPI Entry** — enter/edit monthly CPI + interest rate (writes `macro_monthly`).

## Success Criteria

- Report shows latest scoring batch by default; no double-count from the
  append-only `predictions` table.
- CPI Entry upserts one month via `ON CONFLICT(month)`; a re-entered month edits
  in place, a new month inserts.
- One command brings up the app + a public `*.trycloudflare.com` URL serving
  both pages.

## Non-Goals

- No auth / password gate (open for demo, per user decision).
- No new prediction/scoring logic; UI reads existing tables only.
- No named/persistent Cloudflare tunnel; ephemeral quick tunnel only.

## Architecture

New top-level `dashboard/` dir. Streamlit multipage app on port 8501, single
quick tunnel.

```
dashboard/
  Home.py               # entry page = Report (KPI + table + charts)
  pages/1_CPI_Entry.py  # CPI/rate input form + current table
  db.py                 # psycopg2 helpers; pure query/SQL builders
  run.sh                # launch streamlit + cloudflared quick tunnel
```

**DB access:** psycopg2 + `pandas.read_sql`, NOT Spark. Rationale: Spark is a
JVM job with multi-second startup — wrong tool for interactive UI reads. Reuse
`PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD` from `ml/config.py` as the
single config source (no duplicated connection constants).

**New deps** (append to `requirements.txt`): `streamlit`, `psycopg2-binary`.

## Data Sources (existing tables)

- `predictions` (append-only, no PK): id, district, area_m2, listing_ppm2,
  predicted_ppm2, predicted_total_vnd, undervalued_ratio, is_undervalued, url,
  model_version, scored_at.
- `macro_monthly` (PK month): month `YYYY-MM`, cpi, rate.

## Component: db.py

Pure, testable helpers separated from Streamlit rendering:

- `get_connection()` — psycopg2 connect from `ml/config.py` PG_* values.
- `list_batches()` → `SELECT DISTINCT model_version, scored_at ... ORDER BY
  scored_at DESC` — feeds the batch selector.
- `load_predictions(model_version, scored_at)` → DataFrame filtered to one batch.
- `load_macro()` → full `macro_monthly` ordered by month.
- `upsert_macro(month, cpi, rate)` → parameterized
  `INSERT ... ON CONFLICT(month) DO UPDATE`. Params bound (no string
  interpolation into SQL).
- `month_options()` → list of `YYYY-MM` from 2025-01 through +12 months of
  today; used by the CPI dropdown.

## Component: Report (Home.py)

- **Batch selector:** dropdown of (model_version, scored_at); default = latest
  (max scored_at). Prevents append-only double-count.
- **KPI tiles:** rows scored, % undervalued, avg predicted ppm2, active
  model_version + scored_at.
- **Table:** district, area_m2, listing_ppm2, predicted_ppm2,
  undervalued_ratio, clickable url. Filters: district multiselect +
  undervalued-only toggle.
- **Charts:** predicted-vs-listing ppm2 scatter; undervalued count by district
  bar; ppm2 distribution histogram.

## Component: CPI Entry (pages/1_CPI_Entry.py)

- **Form:** month **dropdown** (from `month_options()`), cpi (float), rate
  (float). Selecting an existing month prefills its cpi/rate (edit mode);
  a month not yet in the table starts blank (add mode).
- **Submit:** `upsert_macro(...)`. Success/error message shown inline.
- **Below form:** current `macro_monthly` table + cpi/rate line chart, so the
  editor sees current state.
- **Validation:** cpi/rate must be positive numbers; month always valid because
  it comes from the dropdown. Bad input rejected with a visible error.

## Component: run.sh

1. Verify `cloudflared` present; if missing on macOS, `brew install cloudflared`.
2. Start `streamlit run Home.py --server.port 8501` (background).
3. Start `cloudflared tunnel --url http://localhost:8501`.
4. Print the public `*.trycloudflare.com` URL.

## Error Handling

- DB connection failure → dashboard shows a clear error banner, not a stack
  trace dump.
- Empty `predictions` → report renders "no data yet" instead of erroring.
- CPI upsert failure → inline error with the DB message; form state preserved.

## Testing

- Unit-test pure logic in `db.py`: `upsert_macro` SQL builder (asserts
  `ON CONFLICT (month) DO UPDATE` present, params bound — encodes the
  edit-in-place intent, Rule 9), `month_options()` range boundaries, batch/
  filter query construction.
- Charts and UI = manual demo verification; not falsely claimed as tested.
- Tests may run against the live docker-compose Postgres (already up on 5432).

## Security Note

The trycloudflare URL is fully public: anyone with the link can read the report
and write CPI rows. Accepted for demo scope. If this ever outlives the demo, add
a shared-secret gate before re-exposing.
