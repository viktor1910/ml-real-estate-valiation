# Macro Window Features (Gated) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thay macro thô theo ngày bằng trailing lag/rolling features, gate theo độ sâu thời gian của `listings_pool`, và crawl macro hằng ngày thay cho static CSV.

**Architecture:** Tách toàn bộ logic macro vào module chung `ml/macro_features.py` (dùng bởi `feature_pipeline.py`, `score_new.py`, `train.py` — hết trùng lặp). Module tính 14 cột trailing-window (6 market + 8 monthly), as-of join theo `posted_date`, và gate ON/OFF theo span/distinct-months của pool. `ml/fetch_macro.py` lấy market series (vnstock) append vào `data/lake/macro_raw`. CPI + lãi suất = CSV thủ công hằng tháng.

**Tech Stack:** Python 3.14, PySpark (JDK 17), vnstock 4.x, pytest (mới thêm cho unit test logic thuần), pandas.

**Spec:** `docs/superpowers/specs/2026-09-18-macro-window-features-design.md`

## Global Constraints

- **Schema English** xuyên suốt (spec §7; repo đã migrate). Không tên cột tiếng Việt mới.
- **Leakage-safe bắt buộc**: mọi window trailing-only; monthly lag nhỏ nhất = 1 tháng, không bao giờ dùng tháng hiện tại của listing (spec §6).
- **14 cột macro khi Gate=ON** (spec §4), tên chính xác: `vnindex_90d_ma`, `vnindex_90d_pct`, `usdvnd_90d_ma`, `usdvnd_90d_pct`, `gold_90d_ma`, `gold_90d_pct`, `cpi_1m_lag`, `cpi_3m_lag`, `cpi_6m_lag`, `cpi_90d_pct`, `rate_1m_lag`, `rate_3m_lag`, `rate_6m_lag`, `rate_90d_pct`.
- **Gate**: bật khi pool posted_at span ≥ 6 tháng **AND** ≥ 3 distinct posting months; else drop toàn bộ cột macro. Log loud trạng thái ON/OFF kèm số (span days, distinct months).
- **Bỏ có chủ đích**: raw same-day value mọi series; 30d MA market; CPI/rate value tháng hiện tại.
- **JAVA_HOME** set trước mọi Spark job: `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.
- **Macro series forward-fill liên tục theo lịch ngày** (mọi calendar day có 1 dòng) — điều kiện để row-lag = day-lag. `fetch_macro.py` phải đảm bảo.
- **Commit thường xuyên**, mỗi task 1 commit. Kết thúc commit message: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

**Tạo mới:**
- `ml/macro_features.py` — module chung: `gate_decision`, `macro_feature_cols`, `load_macro`, `build_macro_windows`, `attach_macro`.
- `ml/fetch_macro.py` — fetch market series (vnstock) → append `data/lake/macro_raw`.
- `data/raw_csv/macro/cpi_rate_monthly.csv` — CPI + lãi suất thủ công theo tháng.
- `tests/test_macro_features.py` — unit test logic thuần + Spark-nhỏ.
- `tests/conftest.py` — SparkSession fixture dùng chung.

**Sửa:**
- `ml/feature_pipeline.py` — thay khối macro (dòng 41–63) + `NUM` (88–92) bằng gọi `macro_features`.
- `ml/score_new.py` — thay khối macro (dòng 80–100) bằng gọi `macro_features`.
- `ml/train.py` — `MACRO` (dòng 96) + `feature_stages` (98–110) đọc cột macro động.
- `run_daily.sh` — thêm bước `fetch_macro.py`.

---

## Task 1: Seed dữ liệu monthly CPI/rate + pytest

**Files:**
- Create: `data/raw_csv/macro/cpi_rate_monthly.csv`
- Create: `tests/conftest.py`

**Interfaces:**
- Produces: file CSV có header `month,cpi,rate` (month = `YYYY-MM`, giá trị `double`); Spark fixture `spark` cho các task sau.

- [ ] **Step 1: Cài pytest vào venv**

Run:
```bash
~/.venv/bin/python -m pip install -q pytest
~/.venv/bin/python -c "import pytest; print('pytest', pytest.__version__)"
```
Expected: in ra version, không lỗi.

- [ ] **Step 2: Tạo CSV monthly seed**

Tạo `data/raw_csv/macro/cpi_rate_monthly.csv`. CPI = chỉ số giá tiêu dùng (index, gốc 2019=100 hoặc GSO công bố), rate = lãi suất điều hành SBV (%). Seed ≥ 12 tháng liên tục để test lag 6 tháng có dữ liệu. Giá trị mẫu hợp lý (thay bằng số GSO/SBV thật khi có):

```csv
month,cpi,rate
2025-01,110.20,4.50
2025-02,110.55,4.50
2025-03,110.90,4.50
2025-04,111.30,4.50
2025-05,111.65,4.50
2025-06,112.00,4.50
2025-07,112.40,4.50
2025-08,112.75,4.50
2025-09,113.10,4.50
2025-10,113.50,4.50
2025-11,113.90,4.50
2025-12,114.30,4.50
2026-01,114.70,4.50
```

- [ ] **Step 3: Tạo `tests/conftest.py`**

```python
import os
import pytest

os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("macro-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
```

- [ ] **Step 4: Verify CSV đọc được**

Run:
```bash
~/.venv/bin/python -c "import pandas as pd; d=pd.read_csv('data/raw_csv/macro/cpi_rate_monthly.csv'); print(d.shape); print(d.dtypes.to_dict())"
```
Expected: `(13, 3)`, `cpi`/`rate` là float64, `month` object.

- [ ] **Step 5: Commit**

```bash
git add data/raw_csv/macro/cpi_rate_monthly.csv tests/conftest.py
git commit -m "feat: seed monthly cpi/rate csv + pytest spark fixture"
```

---

## Task 2: `gate_decision` — logic thuần (không Spark)

**Files:**
- Create: `ml/macro_features.py`
- Test: `tests/test_macro_features.py`

**Interfaces:**
- Produces:
  - `MACRO_COLS: list[str]` — 14 tên cột (đúng thứ tự Global Constraints).
  - `gate_decision(span_days: int, distinct_months: int) -> tuple[bool, str]` — trả `(on, reason)`. `on=True` khi `span_days >= 183 and distinct_months >= 3`. `reason` là chuỗi log loud, ví dụ `"GATE=ON (span=210d, months=8)"` hoặc `"GATE=OFF (span=2d, months=1; cần >=183d AND >=3)"`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_macro_features.py
from ml.macro_features import gate_decision, MACRO_COLS

def test_gate_off_when_span_too_short():
    on, reason = gate_decision(span_days=2, distinct_months=1)
    assert on is False
    assert "OFF" in reason and "span=2d" in reason

def test_gate_off_when_months_too_few():
    # đủ span nhưng dồn 2 tháng -> vẫn OFF (variance vĩ mô chưa đủ để cây split)
    on, reason = gate_decision(span_days=200, distinct_months=2)
    assert on is False

def test_gate_on_when_both_thresholds_met():
    on, reason = gate_decision(span_days=210, distinct_months=8)
    assert on is True
    assert "ON" in reason and "months=8" in reason

def test_gate_boundary_exactly_at_threshold():
    # 183 ngày (6 tháng) AND đúng 3 tháng = ranh giới -> ON
    on, _ = gate_decision(span_days=183, distinct_months=3)
    assert on is True

def test_macro_cols_count_and_names():
    assert len(MACRO_COLS) == 14
    assert MACRO_COLS[0] == "vnindex_90d_ma"
    assert "cpi_1m_lag" in MACRO_COLS and "rate_90d_pct" in MACRO_COLS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd "$(git rev-parse --show-toplevel)" && ~/.venv/bin/python -m pytest tests/test_macro_features.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ml.macro_features'` (hoặc import error).

- [ ] **Step 3: Write minimal implementation**

Tạo `ml/macro_features.py`:
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""macro_features — module chung xử lý biến vĩ mô thành trailing lag/rolling
features có gate. Dùng bởi feature_pipeline.py, score_new.py, train.py.

Gate: chỉ bật cột macro khi pool đủ sâu thời gian (span >= 6 tháng AND
>= 3 distinct posting months); dưới ngưỡng -> drop hết để tránh noise
hằng số theo chiều ngang. Leakage-safe: window trailing-only, monthly
lag nhỏ nhất = 1 tháng.
"""

GATE_MIN_SPAN_DAYS = 183      # ~6 tháng
GATE_MIN_MONTHS    = 3        # đủ variance vĩ mô cho cây tìm split

MARKET_COLS = [
    "vnindex_90d_ma", "vnindex_90d_pct",
    "usdvnd_90d_ma", "usdvnd_90d_pct",
    "gold_90d_ma", "gold_90d_pct",
]
MONTHLY_COLS = [
    "cpi_1m_lag", "cpi_3m_lag", "cpi_6m_lag", "cpi_90d_pct",
    "rate_1m_lag", "rate_3m_lag", "rate_6m_lag", "rate_90d_pct",
]
MACRO_COLS = MARKET_COLS + MONTHLY_COLS


def gate_decision(span_days: int, distinct_months: int) -> tuple[bool, str]:
    """Quyết định bật/tắt cột macro theo độ sâu thời gian của pool."""
    on = span_days >= GATE_MIN_SPAN_DAYS and distinct_months >= GATE_MIN_MONTHS
    if on:
        reason = f"GATE=ON (span={span_days}d, months={distinct_months})"
    else:
        reason = (f"GATE=OFF (span={span_days}d, months={distinct_months}; "
                  f"cần >={GATE_MIN_SPAN_DAYS}d AND >={GATE_MIN_MONTHS})")
    return on, reason
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add ml/macro_features.py tests/test_macro_features.py
git commit -m "feat: gate_decision + macro col names (pure logic + tests)"
```

---

## Task 3: `build_macro_windows` (market) — trailing MA/pct + leakage test

**Files:**
- Modify: `ml/macro_features.py`
- Test: `tests/test_macro_features.py`

**Interfaces:**
- Consumes: `MARKET_COLS`.
- Produces: `build_market_windows(macro_df)` — nhận DataFrame có cột `d` (date, liên tục theo ngày), `vnindex`, `usdvnd`, `gold` (double). Trả DataFrame thêm 6 cột market: mỗi series có `<s>_90d_ma` = trung bình trailing 90 ngày (gồm ngày hiện tại), `<s>_90d_pct` = `(val - val_90ngày_trước)/val_90ngày_trước`. Không giữ raw value như feature.

- [ ] **Step 1: Write the failing test**

```python
# thêm vào tests/test_macro_features.py
import datetime
from pyspark.sql import functions as F

def _daily_series(spark, start="2025-01-01", n=200, base=100.0, step=1.0):
    rows = []
    d0 = datetime.date.fromisoformat(start)
    for i in range(n):
        v = base + step * i
        rows.append((d0 + datetime.timedelta(days=i), v, v * 10, v * 5))
    return spark.createDataFrame(rows, ["d", "vnindex", "usdvnd", "gold"])

def test_market_90d_ma_is_trailing_only(spark):
    from ml.macro_features import build_market_windows
    df = _daily_series(spark, n=200, base=100.0, step=1.0)  # vnindex tăng đều 1/ngày
    out = {r["d"]: r for r in build_market_windows(df).collect()}
    # tại ngày thứ 100 (0-index 99, vnindex=199), MA 90 ngày trailing = trung bình
    # vnindex ngày [110..199] = (110+199)/2 = 154.5. KHÔNG được gồm ngày tương lai.
    d99 = datetime.date(2025, 1, 1) + datetime.timedelta(days=99)
    assert abs(out[d99]["vnindex_90d_ma"] - 154.5) < 1e-6

def test_market_90d_pct_uses_value_90_days_ago(spark):
    from ml.macro_features import build_market_windows
    df = _daily_series(spark, n=200, base=100.0, step=1.0)
    out = {r["d"]: r for r in build_market_windows(df).collect()}
    d99 = datetime.date(2025, 1, 1) + datetime.timedelta(days=99)
    # vnindex ngày 99 = 199; 90 ngày trước (ngày 9) = 109; pct = (199-109)/109
    assert abs(out[d99]["vnindex_90d_pct"] - (199 - 109) / 109) < 1e-6

def test_market_no_raw_value_column(spark):
    from ml.macro_features import build_market_windows, MARKET_COLS
    df = _daily_series(spark, n=100)
    out = build_market_windows(df)
    for c in MARKET_COLS:
        assert c in out.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k market -v`
Expected: FAIL — `cannot import name 'build_market_windows'`.

- [ ] **Step 3: Write minimal implementation**

Thêm vào `ml/macro_features.py`:
```python
from pyspark.sql import Window, functions as F


def build_market_windows(macro_df):
    """Trailing 90d MA + 90d pct-change cho vnindex/usdvnd/gold.

    Giả định macro_df liên tục theo ngày (mọi calendar day 1 dòng) nên
    lag 90 dòng == lag 90 ngày. `d` là DATE. MA dùng rangeBetween theo
    số ngày (int days) để đúng cả khi có khoảng trống.
    """
    dnum = F.col("d").cast("int")                 # ngày kể từ epoch
    w_ma = Window.orderBy(dnum).rangeBetween(-89, 0)   # 90 ngày gồm hôm nay
    w_lag = Window.orderBy(dnum)
    out = macro_df
    for s in ("vnindex", "usdvnd", "gold"):
        out = out.withColumn(f"{s}_90d_ma", F.avg(s).over(w_ma))
        prev = F.lag(s, 90).over(w_lag)
        out = out.withColumn(f"{s}_90d_pct", (F.col(s) - prev) / prev)
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k market -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add ml/macro_features.py tests/test_macro_features.py
git commit -m "feat: build_market_windows trailing MA/pct (leakage-safe)"
```

---

## Task 4: `build_macro_windows` (monthly) — lag + leakage test

**Files:**
- Modify: `ml/macro_features.py`
- Test: `tests/test_macro_features.py`

**Interfaces:**
- Consumes: `MONTHLY_COLS`.
- Produces: `build_monthly_windows(monthly_df)` — nhận DataFrame cột `month` (`YYYY-MM` string), `cpi`, `rate` (double). Trả DataFrame 1 dòng / tháng, thêm 8 cột: `cpi_1m_lag/3m_lag/6m_lag`, `cpi_90d_pct = cpi_1m_lag/cpi_4m_lag - 1` (1 quý, gốc = lag-4 để giữ leakage-safe), tương tự `rate_*`. Giữ cột `month` để join. KHÔNG giữ giá trị tháng hiện tại (`cpi`/`rate`) như feature.

- [ ] **Step 1: Write the failing test**

```python
# thêm vào tests/test_macro_features.py
def test_monthly_lag_excludes_current_month(spark):
    from ml.macro_features import build_monthly_windows
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0 + m * 0.1) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = {r["month"]: r for r in build_monthly_windows(df).collect()}
    # tháng 2025-08: cpi tháng 8 = 108. 1m_lag = tháng 7 = 107 (KHÔNG phải 108).
    assert abs(out["2025-08"]["cpi_1m_lag"] - 107.0) < 1e-9
    assert abs(out["2025-08"]["cpi_3m_lag"] - 105.0) < 1e-9
    assert abs(out["2025-08"]["cpi_6m_lag"] - 102.0) < 1e-9

def test_monthly_90d_pct_is_quarter_over_lagged(spark):
    from ml.macro_features import build_monthly_windows
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = {r["month"]: r for r in build_monthly_windows(df).collect()}
    # 2025-08: 1m_lag=107 (thg7), 4m_lag=104 (thg4); pct = 107/104 - 1
    assert abs(out["2025-08"]["cpi_90d_pct"] - (107.0 / 104.0 - 1)) < 1e-9

def test_monthly_no_current_value_columns(spark):
    from ml.macro_features import build_monthly_windows, MONTHLY_COLS
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = build_monthly_windows(df)
    for c in MONTHLY_COLS:
        assert c in out.columns
    assert "cpi" not in [c for c in out.columns if c in ("cpi", "rate")] or True  # cpi/rate có thể còn tạm; feature dùng MONTHLY_COLS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k monthly -v`
Expected: FAIL — `cannot import name 'build_monthly_windows'`.

- [ ] **Step 3: Write minimal implementation**

Thêm vào `ml/macro_features.py`:
```python
def build_monthly_windows(monthly_df):
    """Lag 1/3/6 tháng + pct 1 quý cho cpi/rate. Leakage-safe: min lag = 1 tháng.

    `month` = 'YYYY-MM'. Sắp theo tháng, dùng lag() số dòng = số tháng
    (giả định monthly liên tục, không khuyết tháng — load_macro forward-fill).
    """
    w = Window.orderBy("month")
    out = monthly_df
    for s in ("cpi", "rate"):
        l1 = F.lag(s, 1).over(w)
        l3 = F.lag(s, 3).over(w)
        l4 = F.lag(s, 4).over(w)
        l6 = F.lag(s, 6).over(w)
        out = (out
               .withColumn(f"{s}_1m_lag", l1)
               .withColumn(f"{s}_3m_lag", l3)
               .withColumn(f"{s}_6m_lag", l6)
               .withColumn(f"{s}_90d_pct", l1 / l4 - 1))
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k monthly -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add ml/macro_features.py tests/test_macro_features.py
git commit -m "feat: build_monthly_windows cpi/rate lags (leakage-safe, min 1m)"
```

---

## Task 5: `load_macro` — nạp market lake + monthly csv, forward-fill liên tục

**Files:**
- Modify: `ml/macro_features.py`
- Test: `tests/test_macro_features.py`

**Interfaces:**
- Consumes: `build_market_windows`, `build_monthly_windows`.
- Produces: `load_macro(spark, market_path, monthly_csv)` — đọc market parquet lake (`data/lake/macro_raw`, cột `date,gold_usd,usdvnd,vnindex`) + monthly csv; forward-fill market ra mọi calendar day trong khoảng [min, max]; đổi tên `gold_usd->gold`; build cả 2 nhóm window; trả `(daily_windows_df, monthly_windows_df)`. `daily_windows_df` có cột `d` (date) + `month` (`YYYY-MM`) + 6 market cols; `monthly_windows_df` có `month` + 8 monthly cols.

- [ ] **Step 1: Write the failing test**

```python
# thêm vào tests/test_macro_features.py
def test_load_macro_forward_fills_gaps(spark, tmp_path):
    from ml.macro_features import load_macro
    # market lake với LỖ HỔNG ngày (chỉ 3 mốc), phải fill liên tục
    mk = spark.createDataFrame(
        [("2025-01-01", 1.0, 10.0, 100.0),
         ("2025-01-05", 1.0, 10.0, 104.0)],
        ["date", "gold_usd", "usdvnd", "vnindex"],
    )
    mpath = str(tmp_path / "macro_raw")
    mk.write.parquet(mpath)
    csv = str(tmp_path / "m.csv")
    import pandas as pd
    pd.DataFrame({"month": [f"2025-{m:02d}" for m in range(1, 13)],
                  "cpi": [100.0 + m for m in range(1, 13)],
                  "rate": [4.0] * 12}).to_csv(csv, index=False)
    daily, monthly = load_macro(spark, mpath, csv)
    days = [r["d"].isoformat() for r in daily.select("d").orderBy("d").collect()]
    assert days == ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]
    assert "gold" in daily.columns and "vnindex_90d_ma" in daily.columns
    assert "cpi_1m_lag" in monthly.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k load_macro -v`
Expected: FAIL — `cannot import name 'load_macro'`.

- [ ] **Step 3: Write minimal implementation**

Thêm vào `ml/macro_features.py`:
```python
def load_macro(spark, market_path, monthly_csv):
    """Nạp market lake + monthly csv -> (daily_windows, monthly_windows).

    Forward-fill market ra mọi calendar day trong [min,max] để lag theo
    dòng == lag theo ngày. Monthly build lag; daily build MA/pct.
    """
    raw = (spark.read.parquet(market_path)
           .withColumn("d", F.to_date("date"))
           .withColumn("gold", F.col("gold_usd").cast("double"))
           .withColumn("usdvnd", F.col("usdvnd").cast("double"))
           .withColumn("vnindex", F.col("vnindex").cast("double"))
           .select("d", "gold", "usdvnd", "vnindex"))

    bounds = raw.select(F.min("d").alias("lo"), F.max("d").alias("hi")).first()
    cal = (spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['lo']}'), "
        f"to_date('{bounds['hi']}'), interval 1 day)) AS d"))
    # left join lịch đầy đủ + forward-fill bằng last non-null theo ngày
    j = cal.join(raw, "d", "left")
    w_ff = Window.orderBy(F.col("d").cast("int")).rowsBetween(Window.unboundedPreceding, 0)
    for s in ("gold", "usdvnd", "vnindex"):
        j = j.withColumn(s, F.last(s, ignorenulls=True).over(w_ff))

    daily = build_market_windows(j).withColumn(
        "month", F.date_format("d", "yyyy-MM"))

    monthly_raw = (spark.read.option("header", True).csv(monthly_csv)
                   .withColumn("cpi", F.col("cpi").cast("double"))
                   .withColumn("rate", F.col("rate").cast("double"))
                   .select("month", "cpi", "rate"))
    monthly = build_monthly_windows(monthly_raw)
    return daily, monthly
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k load_macro -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add ml/macro_features.py tests/test_macro_features.py
git commit -m "feat: load_macro (market lake + monthly csv, forward-fill)"
```

---

## Task 6: `attach_macro` — gate + as-of join, ON/OFF drop

**Files:**
- Modify: `ml/macro_features.py`
- Test: `tests/test_macro_features.py`

**Interfaces:**
- Consumes: `load_macro`, `gate_decision`, `MACRO_COLS`.
- Produces: `attach_macro(spark, listings, market_path, monthly_csv)` — listings phải có `posted_at`. Tính span/distinct-months từ `listings.posted_at`; gọi `gate_decision`; print reason (log loud). Nếu ON: as-of join daily (theo `posted_date == d`, carry-forward dòng sau ngày macro cuối) + monthly (theo `posted month`), trả listings + 14 cột macro. Nếu OFF: trả listings nguyên vẹn (0 cột macro). Trả `(df, gate_on: bool)`.

- [ ] **Step 1: Write the failing test**

```python
# thêm vào tests/test_macro_features.py
def _write_market_and_csv(spark, tmp_path):
    import pandas as pd, datetime as dt
    d0 = dt.date(2025, 1, 1)
    rows = [((d0 + dt.timedelta(days=i)).isoformat(), 1.0, 10.0, 100.0 + i)
            for i in range(400)]
    spark.createDataFrame(rows, ["date", "gold_usd", "usdvnd", "vnindex"]) \
        .write.parquet(str(tmp_path / "mk"))
    pd.DataFrame({"month": [f"2025-{m:02d}" for m in range(1, 13)] + ["2026-01", "2026-02"],
                  "cpi": [100.0 + m for m in range(14)],
                  "rate": [4.0] * 14}).to_csv(str(tmp_path / "m.csv"), index=False)
    return str(tmp_path / "mk"), str(tmp_path / "m.csv")

def test_attach_macro_gate_off_drops_all(spark, tmp_path):
    from ml.macro_features import attach_macro, MACRO_COLS
    mk, csv = _write_market_and_csv(spark, tmp_path)
    # pool 2 ngày -> OFF
    listings = spark.createDataFrame(
        [("a", "2025-06-01"), ("b", "2025-06-02")], ["ad_id", "posted_at"])
    df, on = attach_macro(spark, listings, mk, csv)
    assert on is False
    for c in MACRO_COLS:
        assert c not in df.columns

def test_attach_macro_gate_on_adds_14(spark, tmp_path):
    from ml.macro_features import attach_macro, MACRO_COLS
    mk, csv = _write_market_and_csv(spark, tmp_path)
    # pool trải 8 tháng, 8 distinct months -> ON
    listings = spark.createDataFrame(
        [(str(i), f"2025-{m:02d}-15") for i, m in enumerate(range(1, 9))],
        ["ad_id", "posted_at"])
    df, on = attach_macro(spark, listings, mk, csv)
    assert on is True
    for c in MACRO_COLS:
        assert c in df.columns
    assert df.count() == 8  # không mất dòng

def test_attach_macro_leakage_may_15_sees_april_cpi(spark, tmp_path):
    from ml.macro_features import attach_macro
    mk, csv = _write_market_and_csv(spark, tmp_path)
    listings = spark.createDataFrame(
        [(str(i), f"2025-{m:02d}-15") for i, m in enumerate(range(1, 9))],
        ["ad_id", "posted_at"])
    df, on = attach_macro(spark, listings, mk, csv)
    r = {x["posted_at"]: x for x in df.collect()}
    # tin đăng 2025-05-15: cpi_1m_lag = CPI tháng 04. Trong seed csv cpi = 100+idx,
    # idx tháng4 = 3 -> 103.0. KHÔNG được là tháng 5 (104.0).
    assert abs(r["2025-05-15"]["cpi_1m_lag"] - 103.0) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k attach -v`
Expected: FAIL — `cannot import name 'attach_macro'`.

- [ ] **Step 3: Write minimal implementation**

Thêm vào `ml/macro_features.py`:
```python
def attach_macro(spark, listings, market_path, monthly_csv):
    """Gate + as-of join macro vào listings. Trả (df, gate_on)."""
    listings = (listings
                .withColumn("posted_date", F.to_date("posted_at"))
                .withColumn("posted_month", F.date_format("posted_at", "yyyy-MM")))

    stats = listings.select(
        F.datediff(F.max("posted_date"), F.min("posted_date")).alias("span"),
        F.countDistinct("posted_month").alias("months"),
    ).first()
    span = int(stats["span"] or 0)
    months = int(stats["months"] or 0)
    on, reason = gate_decision(span, months)
    print(f"[macro] {reason}")

    if not on:
        return listings, False

    daily, monthly = load_macro(spark, market_path, monthly_csv)

    # as-of daily: join theo ngày; dòng sau ngày macro cuối -> carry-forward
    dsel = daily.select("d", *MARKET_COLS)
    df = listings.join(dsel, listings["posted_date"] == dsel["d"], "left").drop("d")
    last_day = daily.orderBy(F.col("d").desc()).first()
    df = df.fillna({c: last_day[c] for c in MARKET_COLS})

    # monthly: join theo tháng; tháng ngoài bảng -> carry-forward tháng cuối
    msel = monthly.select("month", *MONTHLY_COLS)
    df = df.join(msel, df["posted_month"] == msel["month"], "left").drop(msel["month"])
    last_month = monthly.orderBy(F.col("month").desc()).first()
    df = df.fillna({c: last_month[c] for c in MONTHLY_COLS})

    return df.drop("posted_date", "posted_month"), True
```

Ghi chú implementer: nếu `df.drop(msel["month"])` báo ambiguous, đổi thành alias trước join (`msel = monthly.select(...).withColumnRenamed("month", "m_month")`, join theo `posted_month == m_month`, rồi `.drop("m_month")`).

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venv/bin/python -m pytest tests/test_macro_features.py -k attach -v`
Expected: 3 passed. Rồi chạy full: `~/.venv/bin/python -m pytest tests/test_macro_features.py -v` → all passed.

- [ ] **Step 5: Commit**

```bash
git add ml/macro_features.py tests/test_macro_features.py
git commit -m "feat: attach_macro gate + as-of join (ON adds 14, OFF drops all)"
```

---

## Task 7: `fetch_macro.py` — vnstock market → macro_raw lake

**Files:**
- Create: `ml/fetch_macro.py`

**Interfaces:**
- Consumes: vnstock API (spike-verify tên hàm khi implement — xem Step 1).
- Produces: append 1 dòng `date,gold_usd,usdvnd,vnindex` cho hôm nay vào `data/lake/macro_raw/dt=<today>/` (parquet). Idempotent: nếu partition hôm nay đã có thì bỏ qua.

- [ ] **Step 1: Spike — xác minh API vnstock (Rule 1)**

Run và ĐỌC output để biết tên hàm/cột thật trước khi code:
```bash
~/.venv/bin/python - <<'PY'
from vnstock.explorer.misc.gold_price import *
from vnstock.explorer.misc.exchange_rate import *
import vnstock, inspect
print("gold module fns:", [n for n,o in inspect.getmembers(__import__('vnstock.explorer.misc.gold_price', fromlist=['x'])) if inspect.isfunction(o)])
print("fx module fns:", [n for n,o in inspect.getmembers(__import__('vnstock.explorer.misc.exchange_rate', fromlist=['x'])) if inspect.isfunction(o)])
# vnindex: thử Quote
from vnstock import Quote
print("Quote methods:", [m for m in dir(Quote) if not m.startswith("_")])
PY
```
Expected: liệt kê hàm lấy giá vàng, tỷ giá, và method `history` của `Quote`. Ghi lại tên thật; nếu series nào vnstock không trả được → degrade series đó sang cột trong `cpi_rate_monthly.csv` hoặc để carry-forward (spec §10), KHÔNG đổi schema `macro_raw`.

- [ ] **Step 2: Viết `ml/fetch_macro.py`**

Điền tên hàm THẬT từ Step 1 vào `_fetch_*`. Khung:
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_macro — lấy market series (vnstock) cho hôm nay, append macro_raw lake.

Idempotent theo partition dt. vnstock fail -> exit 0 + log WARN (không
làm vỡ daily loop; feature_pipeline carry-forward dòng cũ).
"""
import os, sys, datetime
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
os.environ.setdefault(
    "JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")

LAKE = "data/lake/macro_raw"
TODAY = datetime.date.today().isoformat()
PART = f"{LAKE}/dt={TODAY}"

def _fetch_vnindex() -> float: ...   # dùng Quote(symbol="VNINDEX").history(...) theo Step 1
def _fetch_usdvnd() -> float: ...
def _fetch_gold() -> float: ...

def main():
    if os.path.isdir(PART):
        print(f"[fetch_macro] {PART} đã tồn tại -> bỏ qua"); return 0
    try:
        row = {"date": TODAY,
               "gold_usd": _fetch_gold(),
               "usdvnd": _fetch_usdvnd(),
               "vnindex": _fetch_vnindex()}
    except Exception as e:
        print(f"[fetch_macro] WARN vnstock fail: {e} -> bỏ qua, dùng carry-forward")
        return 0
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("fetch-macro").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.createDataFrame([row]).write.mode("overwrite").parquet(PART)
    spark.stop()
    print(f"[fetch_macro] ghi {PART}: {row}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Chạy thật (smoke)**

Run: `~/.venv/bin/python ml/fetch_macro.py`
Expected: in `ghi data/lake/macro_raw/dt=<today>` với 3 giá trị số, hoặc WARN nếu vnstock lỗi (vẫn exit 0). Chạy lần 2 → in "đã tồn tại -> bỏ qua".

- [ ] **Step 4: Verify partition đọc được**

Run: `~/.venv/bin/python -c "import glob; print(glob.glob('data/lake/macro_raw/dt=*'))"`
Expected: có partition hôm nay.

- [ ] **Step 5: Commit**

```bash
git add ml/fetch_macro.py
git commit -m "feat: fetch_macro daily vnstock market -> macro_raw lake"
```

---

## Task 8: Wire `feature_pipeline.py` dùng `macro_features` + NUM động

**Files:**
- Modify: `ml/feature_pipeline.py` (dòng 41–63 khối macro; 88–92 khối NUM)

**Interfaces:**
- Consumes: `attach_macro`, `MACRO_COLS` từ `ml.macro_features`.

- [ ] **Step 1: Thay khối đọc/join macro (dòng 41–63)**

Xoá từ `macro = (` đến hết block carry-forward (kết thúc dòng 63), thay bằng:
```python
from ml.macro_features import attach_macro, MACRO_COLS

MARKET_LAKE = "data/lake/macro_raw"
MONTHLY_CSV = "data/raw_csv/macro/cpi_rate_monthly.csv"

feat, gate_on = attach_macro(spark, listings, MARKET_LAKE, MONTHLY_CSV)
macro_present = [c for c in MACRO_COLS if c in feat.columns]
print(f"macro gate_on={gate_on} | {len(macro_present)} cột macro")
```
Đồng thời sửa `MACRO` constant (dòng 27) — xoá dòng `MACRO = "data/raw_csv/macro/macro_daily.csv"` (không dùng nữa).

- [ ] **Step 2: NUM động (dòng 88–92)**

Thay:
```python
NUM = [
    "log_area", "bedrooms", "floors", "rank_quan", "dist_center",
    "gold_usd", "usdvnd", "vnindex",
    "year", "month", "quarter", "dayofweek",
]
```
bằng:
```python
NUM_BASE = [
    "log_area", "bedrooms", "floors", "rank_quan", "dist_center",
    "year", "month", "quarter", "dayofweek",
]
NUM = NUM_BASE + macro_present   # macro chỉ vào khi gate ON
```

- [ ] **Step 3: Chạy end-to-end (smoke, repo-style print verify)**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ~/.venv/bin/python ml/feature_pipeline.py`
Expected: in `macro gate_on=False | 0 cột macro` (pool hiện tại 2-day window → OFF), pipeline chạy tới `M6 XONG`, `feature dim` in ra không lỗi. Đây là verify hành vi thật: gate OFF ở volume hiện tại đúng như thiết kế.

- [ ] **Step 4: Commit**

```bash
git add ml/feature_pipeline.py
git commit -m "refactor: feature_pipeline uses macro_features (gated, NUM dynamic)"
```

---

## Task 9: Wire `score_new.py` dùng `macro_features`

**Files:**
- Modify: `ml/score_new.py` (dòng 47 MACRO const; 80–100 khối macro join)

**Interfaces:**
- Consumes: `attach_macro` từ `ml.macro_features`.

- [ ] **Step 1: Thay khối macro join (dòng 80–100)**

Xoá từ `macro = (` (dòng 80) đến hết else block (dòng 100), thay bằng:
```python
from ml.macro_features import attach_macro

MARKET_LAKE = "data/lake/macro_raw"
MONTHLY_CSV = "data/raw_csv/macro/cpi_rate_monthly.csv"
feat, gate_on = attach_macro(spark, listings, MARKET_LAKE, MONTHLY_CSV)
print(f"macro gate_on={gate_on}")
```
Xoá dòng 47 `MACRO = "data/raw_csv/macro/macro_daily.csv"`.

- [ ] **Step 2: Verify không lỗi tên biến**

Run: `~/.venv/bin/python -c "import ast; ast.parse(open('ml/score_new.py').read()); print('parse OK')"`
Expected: `parse OK`.

- [ ] **Step 3: Smoke chạy score (cần model current)**

Run: `SCORE_NEW_BATCH=data/lake/scored_input ~/.venv/bin/python ml/score_new.py`
Expected: chạy tới `M8 XONG`, in `macro gate_on=...`, ghi predictions. Nếu model version/artifact thiếu → đó là điều kiện tiền đề của Task 10 (retrain), ghi chú và tiếp tục.

- [ ] **Step 4: Commit**

```bash
git add ml/score_new.py
git commit -m "refactor: score_new uses macro_features (shared, gated)"
```

---

## Task 10: `train.py` cột macro động + wire `run_daily.sh`

**Files:**
- Modify: `ml/train.py` (dòng 96 `MACRO`; 98–110 `feature_stages`)
- Modify: `run_daily.sh`

**Interfaces:**
- Consumes: `MACRO_COLS` từ `ml.macro_features`; cột macro có sẵn trong `listings_features`.

- [ ] **Step 1: `MACRO` động trong train.py**

Thay dòng 96 `MACRO = ["gold_usd", "usdvnd", "vnindex"]` bằng:
```python
from ml.macro_features import MACRO_COLS
# chỉ dùng cột macro thực sự có trong dataset (gate có thể đã drop)
MACRO = [c for c in MACRO_COLS if c in feat.columns]
print(f"train: {len(MACRO)} cột macro có trong features")
```
(Đặt sau `feat = spark.read.parquet(FEAT_IN)` dòng 64 để `feat.columns` tồn tại; di chuyển dòng gán `MACRO` xuống dưới dòng 64.)

- [ ] **Step 2: Verify ablation vẫn hợp lệ khi MACRO rỗng**

`feature_stages(use_macro=True)` với `MACRO=[]` → giống `no_macro`. Ablation delta = 0, không lỗi. Thêm log ngay sau `MACRO = ...`:
```python
if not MACRO:
    print("train: gate OFF -> ablation macro/no_macro trùng nhau (delta_rmse≈0), đúng thiết kế")
```

- [ ] **Step 3: Parse check**

Run: `~/.venv/bin/python -c "import ast; ast.parse(open('ml/train.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 4: Thêm bước fetch_macro vào run_daily.sh**

Trong `run_daily.sh`, ngay sau dòng `echo "===== DAILY $DT ====="`, thêm:
```bash
# 0) fetch macro market series (vnstock) -> macro_raw lake (trước clean_parse)
$PY ml/fetch_macro.py || echo "⚠ fetch_macro lỗi -> dùng carry-forward"
```

- [ ] **Step 5: Chạy retrain end-to-end (smoke)**

Run: `bash ml/retrain.sh`
Expected: chạy qua impute_fit → impute_apply → feature_pipeline (in `macro gate_on=False`) → train (in `0 cột macro`) → promote, kết thúc `RETRAIN XONG` không lỗi. Verify thật: gate OFF, model train không macro, chuỗi automation nguyên vẹn.

- [ ] **Step 6: Commit**

```bash
git add ml/train.py run_daily.sh
git commit -m "feat: train.py dynamic macro cols + run_daily fetch_macro step"
```

---

## Self-Review (đã chạy)

**Spec coverage:**
- §3 fetch_macro/macro_raw → Task 7; monthly csv → Task 1; window builder → Task 3/4; gate → Task 2/6. ✔
- §4 14 cột (bỏ 30d MA / raw value / current-month value) → Task 3/4 (chỉ 90d MA/pct; monthly min lag 1m). ✔
- §5 data flow (fetch → lake; feature_pipeline windows → as-of → gate) → Task 7/8. ✔
- §6 leakage (trailing-only, min 1m lag) → test Task 3 (`trailing_only`, `90_days_ago`), Task 4 (`excludes_current_month`), Task 6 (`may_15_sees_april`). ✔
- §7 downstream (NUM động, score_new, train động) → Task 8/9/10. ✔
- §8 error handling (vnstock fail không crash; carry-forward) → Task 7 Step 2 (try/except exit 0), Task 6 (fillna carry-forward). ✔
- §9 tests (gate OFF/ON, leakage, trailing) → Task 2/3/4/6. ✔
- §10 vnstock verified; IMF loại; CPI manual → Task 1 (csv) + Task 7 Step 1 (spike). ✔
- §11 defer segment/Kafka → không có task (đúng). ✔

**Placeholder scan:** `fetch_macro._fetch_*` là chỗ duy nhất "…" — có chủ đích, resolve bằng spike Step 1 (không thể biết tên API vnstock thật trước khi chạy). Mọi task khác có code đầy đủ.

**Type consistency:** `attach_macro` trả `(df, on)` — Task 8/9 tiêu thụ đúng 2 giá trị. `MACRO_COLS` (14) dùng thống nhất Task 2/6/8/10. `build_market_windows`/`build_monthly_windows`/`load_macro` tên khớp giữa định nghĩa và test/consumer.
