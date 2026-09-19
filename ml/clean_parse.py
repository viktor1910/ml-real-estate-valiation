#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clean_parse — Piece 1. Parse + lọc scope + target → GIỮ NULL → APPEND pool.

Schema ENGLISH (khớp crawl chotot: spider emit key English, value tiếng Việt).
Đọc listings_raw (Kafka landing) → pool phân vùng dt. KHÔNG impute, KHÔNG IQR
(để dành retrain). Ghi APPEND — mỗi crawl thêm 1 partition.

Quyết định (user 2026-09-18):
- Bỏ cột bathrooms + legal (crawl null 100%).
- Lọc bán bằng ad_type='s' (u = cho thuê).
- interior giữ TEXT (StringIndexer ở train), không map enum.
- Bỏ seed CSV — chỉ train trên crawl.

Run:
    python3 clean_parse.py                       # đọc listings_raw
    python3 clean_parse.py --src <parquet> --dt 2026-09-18
"""

import argparse
import datetime
import os

if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")

from config import build_spark, LAKE

from pyspark.sql import SparkSession, functions as F

DEFAULT_SRC = f"{LAKE}/listings_raw"
POOL = f"{LAKE}/listings_pool"

# cột crawl không có / bỏ: bathrooms+legal (null 100%), text thừa
DROP_COLS = ["bathrooms", "legal", "price_string", "apartment_type",
             "direction", "title", "crawled_at"]

# rank_quan: tier thị trường CỨNG (1=rẻ … 5=đắt) — key = district (giá trị tiếng Việt), 0 leakage.
TIER_QUAN = {
    "Quận 1": 5, "Quận 3": 5,
    "Quận 4": 4, "Quận 5": 4, "Quận 7": 4, "Quận 10": 4,
    "Quận Phú Nhuận": 4, "Quận Bình Thạnh": 4, "Quận Tân Bình": 4,
    "Quận 6": 3, "Quận 11": 3, "Quận Gò Vấp": 3, "Quận Tân Phú": 3,
    "Thành phố Thủ Đức": 3,
    "Quận 8": 2, "Quận 12": 2, "Quận Bình Tân": 2,
    "Huyện Nhà Bè": 1, "Huyện Bình Chánh": 1, "Huyện Hóc Môn": 1, "Huyện Củ Chi": 1,
    "Huyện Cần Giờ": 1,
}


def main():
    ap = argparse.ArgumentParser(description="Parse + scope-filter -> append pool (giữ null)")
    ap.add_argument("--src", default=DEFAULT_SRC)
    ap.add_argument("--dt", default=None, help="Ngày partition (mặc định hôm nay)")
    args = ap.parse_args()

    spark = build_spark("clean_parse-pool")

    raw = spark.read.parquet(args.src)
    print("thô:", raw.count(), "| cột:", len(raw.columns))

    # parse kiểu (tên cột English)
    df = (
        raw
        .withColumn("price", F.col("price").cast("double"))                       # VND
        # "92 m²" -> 92.0
        .withColumn("area", F.regexp_extract(F.col("area"), r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double"))
        .withColumn("bedrooms", F.col("bedrooms").cast("int"))
        .withColumn("floors", F.col("floors").cast("int"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("posted_at", F.to_timestamp("posted_at"))
        # interior giữ nguyên TEXT (StringIndexer ở train)
        .drop(*DROP_COLS)
    )

    # scope: HCM · chỉ bán (giá > 500 triệu; dưới coi là cho thuê) · area > 0 · dedup url
    # Ghi chú: seed CSV không có ad_type -> phân loại bán/thuê bằng ngưỡng giá.
    df = df.filter(F.col("city").contains("Hồ Chí Minh"))
    df = df.filter(F.col("price") > 500_000_000)
    df = df.filter(F.col("area") > 0)
    df = df.dropDuplicates(["url"]).drop("ad_type")

    # target: price_per_m2 (triệu/m²)
    df = df.withColumn("price_per_m2", (F.col("price") / 1_000_000) / F.col("area"))

    # rank_quan tier cứng theo district (quận lạ -> null + cảnh báo)
    from itertools import chain
    tier_map = F.create_map([F.lit(x) for x in chain.from_iterable(TIER_QUAN.items())])
    df = df.withColumn("rank_quan", tier_map[F.col("district")].cast("int"))
    n_unmapped = df.filter(F.col("rank_quan").isNull()).count()
    if n_unmapped:
        miss = [r[0] for r in df.filter(F.col("rank_quan").isNull())
                .select("district").distinct().collect()]
        print(f"⚠ {n_unmapped} dòng quận CHƯA có tier -> thêm vào TIER_QUAN: {miss}")

    # KHÔNG impute, KHÔNG IQR — giữ null. Partition theo ngày ingest.
    dt = args.dt or datetime.date.today().isoformat()
    df = df.withColumn("dt", F.lit(dt))

    n = df.count()
    df.write.mode("append").partitionBy("dt").parquet(POOL)
    print(f"append -> {POOL}/dt={dt} | {n} dòng (null giữ nguyên)")


if __name__ == "__main__":
    main()
