#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
impute_fit — Piece 2a. Học tham số làm sạch MỘT LẦN trên pool → JSON. Chạy khi RETRAIN.

Schema English. Tính:
- dedup url toàn pool (tích lũy nhiều ngày: cùng tin re-crawl → giữ bản mới nhất theo dt).
- IQR bounds (price_per_m2, area) — ngưỡng lọc ngoại lai train-time.
- fill: floors=1 (const), bedrooms=median, interior='UNKNOWN' (const, cho StringIndexer).

Thứ tự: dedup → lọc IQR → tính median trên data đã lọc.

Run:
    python3 impute_fit.py
    python3 impute_fit.py --src data/lake/listings_pool --out models/impute_stats.json
"""

import argparse
import datetime
import json
import os

if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")

from config import build_spark, LAKE, META_DIR

from pyspark.sql import SparkSession, functions as F, Window

DEFAULT_SRC = f"{LAKE}/listings_pool"
DEFAULT_OUT = f"{META_DIR}/impute_stats.json"
IQR_COLS = ("price_per_m2", "area")


def dedup_pool(df):
    """Giữ 1 dòng/url = bản crawl mới nhất (dt lớn nhất)."""
    w = Window.partitionBy("url").orderBy(F.col("dt").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))


def main():
    ap = argparse.ArgumentParser(description="Fit stats làm sạch -> JSON")
    ap.add_argument("--src", default=DEFAULT_SRC)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    spark = build_spark("impute_fit")

    df = dedup_pool(spark.read.parquet(args.src))
    n_in = df.count()

    # IQR bounds + lọc (median tính trên data ĐÃ lọc)
    iqr = {}
    for col in IQR_COLS:
        q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.0)
        lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
        iqr[col] = [lo, hi]
        df = df.filter((F.col(col) >= lo) & (F.col(col) <= hi))
    n_iqr = df.count()

    med_bed = df.approxQuantile("bedrooms", [0.5], 0.0)[0]
    fill = {
        "floors": 1,
        "bedrooms": int(med_bed) if med_bed is not None else 1,
        "interior": "UNKNOWN",
    }

    stats = {
        "fit_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "src": args.src, "n_rows_dedup": n_in, "n_rows_after_iqr": n_iqr,
        "iqr": iqr, "fill": fill,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"dedup {n_in} dòng -> IQR {n_iqr}")
    print(f"stats -> {args.out}")
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
