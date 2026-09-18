#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
impute_apply — Piece 2b. Nạp stats ĐÓNG BĂNG → áp. KHÔNG tính lại. Schema English.

Mode:
- train : dedup pool + lọc IQR (drop ngoại lai) + điền khuyết  -> data train.
- serve : CHỈ điền khuyết, GIỮ mọi dòng (lô hôm nay, không vứt tin cần chấm).

Run:
    python3 impute_apply.py --src data/lake/listings_pool --out data/lake/listings_clean/sale --mode train
    python3 impute_apply.py --src <new_batch> --out data/lake/scored_input --mode serve
"""

import argparse
import json
import os

if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")

os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

from pyspark.sql import SparkSession, functions as F, Window


def dedup_pool(df):
    w = Window.partitionBy("url").orderBy(F.col("dt").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))


def main():
    ap = argparse.ArgumentParser(description="Áp stats đóng băng -> data sạch")
    ap.add_argument("--src", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--stats", default="models/impute_stats.json")
    ap.add_argument("--mode", choices=["train", "serve"], required=True)
    args = ap.parse_args()

    with open(args.stats, encoding="utf-8") as f:
        stats = json.load(f)

    spark = (
        SparkSession.builder.appName(f"impute_apply-{args.mode}")
        .master("local[*]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.src)
    if args.mode == "train":
        df = dedup_pool(df)              # dedup chéo ngày chỉ khi train trên pool
    n_in = df.count()

    if args.mode == "train":
        for col, (lo, hi) in stats["iqr"].items():
            df = df.filter((F.col(col) >= lo) & (F.col(col) <= hi))

    fill = {k: v for k, v in stats["fill"].items() if v is not None}
    df = df.fillna(fill)

    n_out = df.count()
    df.write.mode("overwrite").parquet(args.out)
    print(f"[{args.mode}] {n_in} -> {n_out} dòng | fill={fill} -> {args.out}")


if __name__ == "__main__":
    main()
