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
