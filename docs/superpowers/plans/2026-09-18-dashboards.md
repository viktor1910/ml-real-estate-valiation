# Dashboards + Cloudflare Tunnel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship two Streamlit dashboards (prediction Report + monthly CPI entry) reading/writing the existing Postgres, exposed for demo via a Cloudflare quick tunnel.

**Architecture:** One Streamlit multipage app in `dashboard/` on port 8501. Pure SQL/logic helpers live in `dashboard/db.py` and are unit-tested; page files (`Home.py`, `pages/1_CPI_Entry.py`) render UI and call those helpers. DB access is psycopg2 + `pandas.read_sql` (not Spark). `run.sh` launches Streamlit + `cloudflared` quick tunnel.

**Tech Stack:** Python 3.11, Streamlit, psycopg2-binary, pandas, Postgres (docker-compose), cloudflared.

**Spec:** `docs/superpowers/specs/2026-09-18-dashboards-design.md`

## Global Constraints

- Reuse Postgres connection values from `ml/config.py` (`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`) — no duplicated connection constants.
- No auth/password gate (demo scope, per user decision).
- Read-only against `predictions`; only `macro_monthly` is written, via `INSERT ... ON CONFLICT(month) DO UPDATE`.
- All SQL that takes user input uses bound parameters — never string interpolation.
- Tests may run against the live docker-compose Postgres on `localhost:5432`.
- New deps pinned in `requirements.txt`: `streamlit`, `psycopg2-binary`.

---

### Task 1: Deps + pure helpers in db.py (month_options, upsert SQL)

**Files:**
- Modify: `requirements.txt` (append streamlit + psycopg2-binary)
- Create: `dashboard/db.py`
- Create: `dashboard/__init__.py` (empty, makes it importable)
- Test: `tests/test_dashboard_db.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `UPSERT_MACRO_SQL: str` — the parameterized upsert statement.
  - `month_options(today: date | None = None) -> list[str]` — `YYYY-MM` strings from `2025-01` through 12 months after `today` (inclusive), ascending.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dashboard_db.py
import os, sys
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import db


def test_upsert_sql_is_parameterized_upsert():
    sql = db.UPSERT_MACRO_SQL.lower()
    assert "insert into macro_monthly" in sql
    assert "on conflict (month) do update" in sql
    # bound params, not interpolation
    assert "%s" in sql
    assert "{" not in db.UPSERT_MACRO_SQL and "format" not in db.UPSERT_MACRO_SQL


def test_month_options_range_boundaries():
    opts = db.month_options(today=date(2026, 1, 15))
    assert opts[0] == "2025-01"          # seed start
    assert opts[-1] == "2027-01"         # today + 12 months
    assert "2026-01" in opts
    assert opts == sorted(opts)          # ascending
    assert len(opts) == len(set(opts))   # no dupes
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_dashboard_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'db'` / attribute missing.

- [ ] **Step 3: Write minimal implementation**

```python
# dashboard/db.py
"""DB helpers for the Streamlit dashboards. Pure builders here are unit-tested;
connection-backed queries live below them. PG_* reused from ml/config.py."""
import os, sys
from datetime import date

# ml/ on path so we reuse the single Postgres config source.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD  # noqa: E402

UPSERT_MACRO_SQL = (
    "INSERT INTO macro_monthly (month, cpi, rate) VALUES (%s, %s, %s) "
    "ON CONFLICT (month) DO UPDATE SET cpi = EXCLUDED.cpi, rate = EXCLUDED.rate"
)

SEED_START = date(2025, 1, 1)


def month_options(today=None):
    today = today or date.today()
    end_year, end_month = today.year, today.month
    # +12 months
    end = end_year * 12 + (end_month - 1) + 12
    start = SEED_START.year * 12 + (SEED_START.month - 1)
    return [f"{m // 12:04d}-{m % 12 + 1:02d}" for m in range(start, end + 1)]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_dashboard_db.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Append deps + commit**

Append to `requirements.txt`:
```
streamlit==1.40.2
psycopg2-binary==2.9.10
```
Then:
```bash
.venv/bin/pip install "streamlit==1.40.2" "psycopg2-binary==2.9.10"
git add requirements.txt dashboard/db.py dashboard/__init__.py tests/test_dashboard_db.py
git commit -m "feat(dashboard): pure db helpers + deps"
```

---

### Task 2: Connection-backed query helpers in db.py

**Files:**
- Modify: `dashboard/db.py`
- Test: `tests/test_dashboard_db.py` (add live-Postgres tests)

**Interfaces:**
- Consumes: `UPSERT_MACRO_SQL` (Task 1), PG_* from config.
- Produces:
  - `get_connection()` -> psycopg2 connection.
  - `list_batches() -> pandas.DataFrame` columns `[model_version, scored_at]`, newest first.
  - `load_predictions(model_version, scored_at) -> pandas.DataFrame` — one batch.
  - `load_macro() -> pandas.DataFrame` columns `[month, cpi, rate]` ascending.
  - `upsert_macro(month: str, cpi: float, rate: float) -> None` — executes + commits.

- [ ] **Step 1: Write the failing test** (live Postgres; skips if unreachable)

```python
# append to tests/test_dashboard_db.py
import pytest

def _pg_up():
    try:
        c = db.get_connection(); c.close(); return True
    except Exception:
        return False

pytestmark_live = pytest.mark.skipif(not _pg_up(), reason="Postgres not running")


@pytestmark_live
def test_upsert_then_load_roundtrip():
    db.upsert_macro("2099-12", 999.0, 1.5)     # add
    m = db.load_macro()
    row = m[m["month"] == "2099-12"].iloc[0]
    assert row["cpi"] == 999.0 and row["rate"] == 1.5
    db.upsert_macro("2099-12", 111.0, 2.5)     # edit in place
    m2 = db.load_macro()
    assert (m2["month"] == "2099-12").sum() == 1          # no duplicate row
    assert m2[m2["month"] == "2099-12"].iloc[0]["cpi"] == 111.0


@pytestmark_live
def test_list_batches_columns():
    b = db.list_batches()
    assert list(b.columns) == ["model_version", "scored_at"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_dashboard_db.py -v`
Expected: FAIL — `get_connection` not defined (or skip if PG down — then start `docker compose up -d postgres` first).

- [ ] **Step 3: Write minimal implementation**

```python
# append to dashboard/db.py
import psycopg2
import pandas as pd


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def list_batches():
    q = ("SELECT DISTINCT model_version, scored_at FROM predictions "
         "ORDER BY scored_at DESC")
    with get_connection() as c:
        return pd.read_sql(q, c)


def load_predictions(model_version, scored_at):
    q = ("SELECT district, area_m2, listing_ppm2, predicted_ppm2, "
         "undervalued_ratio, is_undervalued, url, model_version, scored_at "
         "FROM predictions WHERE model_version = %s AND scored_at = %s")
    with get_connection() as c:
        return pd.read_sql(q, c, params=(model_version, scored_at))


def load_macro():
    with get_connection() as c:
        return pd.read_sql("SELECT month, cpi, rate FROM macro_monthly ORDER BY month", c)


def upsert_macro(month, cpi, rate):
    with get_connection() as c:
        with c.cursor() as cur:
            cur.execute(UPSERT_MACRO_SQL, (month, float(cpi), float(rate)))
        c.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker compose up -d postgres && .venv/bin/pytest tests/test_dashboard_db.py -v`
Expected: PASS (roundtrip + columns). Cleanup test row is optional (month 2099-12).

- [ ] **Step 5: Commit**

```bash
git add dashboard/db.py tests/test_dashboard_db.py
git commit -m "feat(dashboard): postgres query + upsert helpers"
```

---

### Task 3: Report page (Home.py)

**Files:**
- Create: `dashboard/Home.py`

**Interfaces:**
- Consumes: `db.list_batches`, `db.load_predictions` (Task 2).
- Produces: runnable Streamlit entry page (manual-verify deliverable).

- [ ] **Step 1: Implement the page**

```python
# dashboard/Home.py
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import streamlit as st
import db

st.set_page_config(page_title="Real Estate Report", layout="wide")
st.title("📊 Prediction Report")

try:
    batches = db.list_batches()
except Exception as e:
    st.error(f"Cannot reach Postgres: {e}")
    st.stop()

if batches.empty:
    st.info("No predictions scored yet.")
    st.stop()

labels = [f"{r.model_version} · {r.scored_at}" for r in batches.itertuples()]
idx = st.selectbox("Scoring batch", range(len(labels)),
                   format_func=lambda i: labels[i])  # default 0 = latest
sel = batches.iloc[idx]
df = db.load_predictions(sel.model_version, sel.scored_at)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows scored", len(df))
c2.metric("% undervalued", f"{100 * df['is_undervalued'].mean():.1f}%" if len(df) else "0%")
c3.metric("Avg predicted ppm2", f"{df['predicted_ppm2'].mean():.1f}" if len(df) else "—")
c4.metric("Model", str(sel.model_version))

districts = sorted(df["district"].dropna().unique())
pick = st.multiselect("District", districts, default=districts)
only_uv = st.checkbox("Undervalued only", value=False)
view = df[df["district"].isin(pick)]
if only_uv:
    view = view[view["is_undervalued"]]

st.dataframe(
    view,
    column_config={"url": st.column_config.LinkColumn("url")},
    use_container_width=True, hide_index=True,
)

st.subheader("Charts")
if not view.empty:
    st.scatter_chart(view, x="listing_ppm2", y="predicted_ppm2")
    st.bar_chart(view[view["is_undervalued"]].groupby("district").size())
    st.bar_chart(view["predicted_ppm2"].value_counts(bins=20).sort_index())
```

- [ ] **Step 2: Manual verify**

Run: `docker compose up -d postgres && .venv/bin/streamlit run dashboard/Home.py`
Expected: page loads, batch dropdown defaults to latest, KPIs + table + charts render (or "No predictions scored yet." if empty). Confirm no stack trace.

- [ ] **Step 3: Commit**

```bash
git add dashboard/Home.py
git commit -m "feat(dashboard): report page"
```

---

### Task 4: CPI Entry page

**Files:**
- Create: `dashboard/pages/1_CPI_Entry.py`

**Interfaces:**
- Consumes: `db.month_options`, `db.load_macro`, `db.upsert_macro`.
- Produces: runnable CPI entry page (manual-verify deliverable).

- [ ] **Step 1: Implement the page**

```python
# dashboard/pages/1_CPI_Entry.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import streamlit as st
import db

st.set_page_config(page_title="CPI Entry", layout="centered")
st.title("📝 Monthly CPI / Rate Entry")

try:
    macro = db.load_macro()
except Exception as e:
    st.error(f"Cannot reach Postgres: {e}")
    st.stop()

existing = {r.month: (r.cpi, r.rate) for r in macro.itertuples()}
options = db.month_options()
# default selection = most recent existing month if any
default_idx = options.index(macro["month"].max()) if not macro.empty else len(options) - 1

with st.form("cpi_form"):
    month = st.selectbox("Month", options, index=default_idx)
    prefill = existing.get(month, (0.0, 0.0))
    cpi = st.number_input("CPI index", min_value=0.0, value=float(prefill[0]), step=0.1)
    rate = st.number_input("Interest rate (%/yr)", min_value=0.0, value=float(prefill[1]), step=0.1)
    submitted = st.form_submit_button("Save")

if submitted:
    if cpi <= 0 or rate <= 0:
        st.error("CPI and rate must be positive.")
    else:
        try:
            db.upsert_macro(month, cpi, rate)
            st.success(f"Saved {month}: cpi={cpi}, rate={rate}")
            macro = db.load_macro()
        except Exception as e:
            st.error(f"Save failed: {e}")

st.subheader("Current macro_monthly")
st.dataframe(macro, use_container_width=True, hide_index=True)
if not macro.empty:
    st.line_chart(macro.set_index("month")[["cpi", "rate"]])
```

Note: the `prefill` reads on each rerun; selecting a month reruns the script so the number inputs update to that month's stored values (edit vs add).

- [ ] **Step 2: Manual verify**

Run: app already up from Task 3 — open the "CPI Entry" page in the sidebar.
Expected: dropdown lists months; picking an existing month prefills its cpi/rate; Save on a new month inserts; re-saving edits in place (table below shows one row, updated); negative input rejected.

- [ ] **Step 3: Commit**

```bash
git add dashboard/pages/1_CPI_Entry.py
git commit -m "feat(dashboard): CPI entry page"
```

---

### Task 5: run.sh — Streamlit + Cloudflare quick tunnel

**Files:**
- Create: `dashboard/run.sh` (executable)

**Interfaces:**
- Consumes: the app from Tasks 3–4.
- Produces: one command that serves both pages on a public URL.

- [ ] **Step 1: Implement the script**

```bash
#!/usr/bin/env bash
# Launch the dashboards + a Cloudflare quick tunnel (public *.trycloudflare.com).
set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8501}"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared not found. Installing (macOS/brew)..."
  brew install cloudflared
fi

# Streamlit in background
../.venv/bin/streamlit run Home.py --server.port "$PORT" --server.headless true &
ST_PID=$!
trap 'kill $ST_PID 2>/dev/null || true' EXIT

# Wait for the port, then open the tunnel (its output prints the public URL)
for _ in $(seq 1 30); do
  if curl -sf "http://localhost:$PORT" >/dev/null 2>&1; then break; fi
  sleep 1
done

echo "Streamlit up on :$PORT — opening Cloudflare quick tunnel..."
cloudflared tunnel --url "http://localhost:$PORT"
```

- [ ] **Step 2: Manual verify**

Run: `docker compose up -d postgres && chmod +x dashboard/run.sh && ./dashboard/run.sh`
Expected: script prints a `https://<random>.trycloudflare.com` URL; opening it shows the Report page; sidebar switches to CPI Entry. Ctrl-C stops both.

- [ ] **Step 3: Commit**

```bash
git add dashboard/run.sh
git commit -m "feat(dashboard): run script with cloudflare quick tunnel"
```

---

## Self-Review

**Spec coverage:**
- Report (KPI/table/charts, batch default latest) → Task 3. ✓
- CPI entry (dropdown, prefill edit/add, upsert, current table+chart, positive validation) → Task 4. ✓
- db.py psycopg2 helpers reusing ml/config PG_* → Tasks 1–2. ✓
- run.sh streamlit + quick tunnel → Task 5. ✓
- Deps in requirements.txt → Task 1. ✓
- Error handling (DB down banner, empty predictions) → Tasks 3–4 (`st.error` + `st.stop`). ✓
- Testing (upsert SQL intent, month_options boundaries, roundtrip) → Tasks 1–2. ✓
- No auth → honored (no gate anywhere). ✓

**Placeholder scan:** none — every step has runnable code/commands.

**Type consistency:** `list_batches`/`load_predictions`/`load_macro` return DataFrames with the columns consumed in Tasks 3–4; `upsert_macro(month, cpi, rate)` signature matches Task 4 call; `month_options()` returns `YYYY-MM` strings used as selectbox options and indexed by `macro["month"].max()`. Consistent.
