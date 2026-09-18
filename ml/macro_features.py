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
