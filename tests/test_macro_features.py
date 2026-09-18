import datetime

from macro_features import gate_decision, MACRO_COLS


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


def _daily_series(spark, start="2025-01-01", n=200, base=100.0, step=1.0):
    rows = []
    d0 = datetime.date.fromisoformat(start)
    for i in range(n):
        v = base + step * i
        rows.append((d0 + datetime.timedelta(days=i), v, v * 10, v * 5))
    return spark.createDataFrame(rows, ["d", "vnindex", "usdvnd", "gold"])


def test_market_90d_ma_is_trailing_only(spark):
    from macro_features import build_market_windows
    df = _daily_series(spark, n=200, base=100.0, step=1.0)  # vnindex tăng đều 1/ngày
    out = {r["d"]: r for r in build_market_windows(df).collect()}
    # tại ngày thứ 100 (0-index 99, vnindex=199), MA 90 ngày trailing =
    # trung bình vnindex ngày [110..199] = (110+199)/2 = 154.5. KHÔNG gồm tương lai.
    d99 = datetime.date(2025, 1, 1) + datetime.timedelta(days=99)
    assert abs(out[d99]["vnindex_90d_ma"] - 154.5) < 1e-6


def test_market_90d_pct_uses_value_90_days_ago(spark):
    from macro_features import build_market_windows
    df = _daily_series(spark, n=200, base=100.0, step=1.0)
    out = {r["d"]: r for r in build_market_windows(df).collect()}
    d99 = datetime.date(2025, 1, 1) + datetime.timedelta(days=99)
    # vnindex ngày 99 = 199; 90 ngày trước (ngày 9) = 109; pct = (199-109)/109
    assert abs(out[d99]["vnindex_90d_pct"] - (199 - 109) / 109) < 1e-6


def test_market_has_all_market_cols(spark):
    from macro_features import build_market_windows, MARKET_COLS
    df = _daily_series(spark, n=100)
    out = build_market_windows(df)
    for c in MARKET_COLS:
        assert c in out.columns


def test_monthly_lag_excludes_current_month(spark):
    from macro_features import build_monthly_windows
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0 + m * 0.1) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = {r["month"]: r for r in build_monthly_windows(df).collect()}
    # 2025-08: cpi tháng 8 = 108. 1m_lag = tháng 7 = 107 (KHÔNG phải 108).
    assert abs(out["2025-08"]["cpi_1m_lag"] - 107.0) < 1e-9
    assert abs(out["2025-08"]["cpi_3m_lag"] - 105.0) < 1e-9
    assert abs(out["2025-08"]["cpi_6m_lag"] - 102.0) < 1e-9


def test_monthly_90d_pct_is_quarter_over_lagged(spark):
    from macro_features import build_monthly_windows
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = {r["month"]: r for r in build_monthly_windows(df).collect()}
    # 2025-08: 1m_lag=107 (thg7), 4m_lag=104 (thg4); pct = 107/104 - 1
    assert abs(out["2025-08"]["cpi_90d_pct"] - (107.0 / 104.0 - 1)) < 1e-9


def test_monthly_has_all_monthly_cols(spark):
    from macro_features import build_monthly_windows, MONTHLY_COLS
    rows = [(f"2025-{m:02d}", 100.0 + m, 4.0) for m in range(1, 13)]
    df = spark.createDataFrame(rows, ["month", "cpi", "rate"])
    out = build_monthly_windows(df)
    for c in MONTHLY_COLS:
        assert c in out.columns
