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

from pyspark.sql import Window, functions as F


def build_market_windows(macro_df):
    """Trailing 90d MA + 90d pct-change cho vnindex/usdvnd/gold.

    Giả định macro_df liên tục theo ngày (mọi calendar day 1 dòng) nên
    lag 90 dòng == lag 90 ngày. `d` là DATE. MA dùng rangeBetween theo
    số ngày (int days) để đúng cả khi có khoảng trống.
    """
    dnum = F.unix_date(F.col("d"))                     # ngày kể từ epoch (int)
    w_ma = Window.orderBy(dnum).rangeBetween(-89, 0)   # 90 ngày gồm hôm nay
    w_lag = Window.orderBy(dnum)
    out = macro_df
    for s in ("vnindex", "usdvnd", "gold"):
        out = out.withColumn(f"{s}_90d_ma", F.avg(s).over(w_ma))
        prev = F.lag(s, 90).over(w_lag)
        out = out.withColumn(f"{s}_90d_pct", (F.col(s) - prev) / prev)
    return out


def build_monthly_windows(monthly_df):
    """Lag 1/3/6 tháng + pct 1 quý cho cpi/rate. Leakage-safe: min lag = 1 tháng.

    `month` = 'YYYY-MM'. Sắp theo tháng, dùng lag() số dòng = số tháng
    (giả định monthly liên tục, không khuyết tháng — load_macro đảm bảo).
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


def gate_decision(span_days: int, distinct_months: int) -> tuple[bool, str]:
    """Quyết định bật/tắt cột macro theo độ sâu thời gian của pool."""
    on = span_days >= GATE_MIN_SPAN_DAYS and distinct_months >= GATE_MIN_MONTHS
    if on:
        reason = f"GATE=ON (span={span_days}d, months={distinct_months})"
    else:
        reason = (f"GATE=OFF (span={span_days}d, months={distinct_months}; "
                  f"cần >={GATE_MIN_SPAN_DAYS}d AND >={GATE_MIN_MONTHS})")
    return on, reason
