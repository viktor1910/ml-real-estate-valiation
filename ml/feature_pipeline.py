#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feature_pipeline

Auto-converted from feature_pipeline.ipynb for deployment.
Spark job: requires JAVA_HOME set to a JDK 17 install, e.g.
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
Run:
    python3 feature_pipeline.py
"""


# # M6 — Feature pipeline + vĩ mô (dùng chung Luồng A & B)
#
# Đọc `listings_clean` (M5, 23 cột) → **as-of join** vĩ mô theo `Ngay dang` → build **MỘT** Spark ML `Pipeline` dùng chung train (M7) & serve (M8), lưu lại.
#
# **Quyết định (user 2026-09-17):**
# - Encoding quận: `rank_quan` ordinal 1–5 (đã có ở M5) — KHÔNG one-hot 21 quận (tránh cộng tuyến).
# - Macro: re-fetch phủ 2025-11 → 2026-09 (data 98% dồn Dec-2025). Tín hiệu vĩ mô yếu trong cụm 11 ngày — chấp nhận, báo cáo trung thực.
#
# **Data reality:** 2156 dòng = 2142 (data_bds, posted Dec-2025, thiếu `Loai BDS`) + 14 (chotot 2026). `Loai BDS` null 98% → giữ qua `handleInvalid=keep` (sẵn cho crawl tương lai).
#
# **Pipeline lưu ở dạng UNFITTED (recipe)** — M7 fit trên train split theo thời gian (tránh leakage scaler/indexer). M6 chỉ chứng minh fit được + lưu recipe.

# ## 1. Config + Java

import os
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    SQLTransformer, StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler,
)

CLEAN     = "data/lake/listings_clean/sale"          # input (M5)
MACRO     = "data/raw_csv/macro/macro_daily.csv"     # input (fetch_macro.py)
FEAT_OUT  = "data/lake/listings_features/sale"       # output: joined dataset (M7 đọc)
PIPE_PATH = "models/feature_pipeline"                # output: unfitted Pipeline recipe

# tâm HCM (Q1, Bến Thành) cho khoảng cách
HCM_LAT, HCM_LON = 10.7769, 106.7009

# ## 2. Đọc `listings_clean` + `macro_daily`

spark = (
    SparkSession.builder.appName("M6-feature-pipeline")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

listings = spark.read.parquet(CLEAN)
print("listings_clean:", listings.count(), "dòng,", len(listings.columns), "cột")

# macro CSV: ép double
macro = (
    spark.read.option("header", True).csv(MACRO)
    .withColumn("d", F.to_date("date"))
    .withColumn("gold_usd", F.col("gold_usd").cast("double"))
    .withColumn("usdvnd",   F.col("usdvnd").cast("double"))
    .withColumn("vnindex",  F.col("vnindex").cast("double"))
    .select("d", "gold_usd", "usdvnd", "vnindex")
)
r = macro.select(F.min("d"), F.max("d")).first()
print("macro_daily:", macro.count(), "ngày,", r[0], "->", r[1])

# ## 3. As-of join vĩ mô theo `Ngay dang`
#
# `macro_daily` đã reindex lịch NGÀY liên tục + forward-fill → giá trị mỗi ngày = phiên gần nhất ĐÃ biết. Equi-join theo ngày `posted_at` = as-of (không dùng số tương lai).

listings = listings.withColumn("posted_date", F.to_date("Ngay dang"))

feat = listings.join(macro, listings["posted_date"] == macro["d"], "left").drop("d")

# kiểm coverage: dòng nào posted_date < đầu chuỗi macro sẽ null
miss = feat.filter(F.col("vnindex").isNull() | F.col("gold_usd").isNull() | F.col("usdvnd").isNull()).count()
pr = feat.select(F.min("posted_date"), F.max("posted_date")).first()
print(f"posted_date {pr[0]} -> {pr[1]} | dòng thiếu macro: {miss}")
if miss:
    print("⚠ có dòng ngoài cửa sổ macro — mở rộng fetch_macro --start")
feat.select("posted_date", "gold_usd", "usdvnd", "vnindex").show(4, False)

# ## 4. Định nghĩa Spark ML `Pipeline` (dùng chung train/serve)
#
# **3 nhóm đặc trưng:**
# 1. **Căn:** `log_area`, `Phong ngu`, `Nha ve sinh`, `So tang`, `rank_quan`, `dist_center` (haversine tới tâm HCM), one-hot `Loai BDS`.
# 2. **Vĩ mô:** `gold_usd`, `usdvnd`, `vnindex` (as-of theo ngày đăng).
# 3. **Thời gian:** `year`, `month`, `quarter`, `dayofweek` từ `Ngay dang`.
#
# Mọi biến đổi per-row nằm TRONG pipeline (SQLTransformer) → serve tái dựng y hệt, không lệch train/serve. As-of join là bước data ngoài pipeline (cần bảng macro).

# 4.1 SQLTransformer: cột phái sinh (tên cột chotot có dấu cách -> backtick)
derive = SQLTransformer(statement="""
    SELECT *,
        log(`Dien tich`)                              AS log_area,
        year(`Ngay dang`)                             AS year,
        month(`Ngay dang`)                            AS month,
        quarter(`Ngay dang`)                          AS quarter,
        dayofweek(`Ngay dang`)                        AS dayofweek,
        6371 * 2 * asin(sqrt(
            power(sin(radians(`Vi do` - 10.7769) / 2), 2) +
            cos(radians(10.7769)) * cos(radians(`Vi do`)) *
            power(sin(radians(`Kinh do` - 106.7009) / 2), 2)
        ))                                            AS dist_center,
        coalesce(`Loai BDS`, 'UNKNOWN')               AS loai_bds_s
    FROM __THIS__
""")

# 4.2 categorical: Loai BDS (98% null hiện tại -> keep; sẵn cho chotot tương lai)
idx = StringIndexer(inputCol="loai_bds_s", outputCol="loai_idx", handleInvalid="keep")
ohe = OneHotEncoder(inputCol="loai_idx", outputCol="loai_ohe", handleInvalid="keep")

# 4.3 gom numeric + one-hot
NUM = [
    "log_area", "Phong ngu", "Nha ve sinh", "So tang", "rank_quan", "dist_center",
    "gold_usd", "usdvnd", "vnindex",
    "year", "month", "quarter", "dayofweek",
]
asm = VectorAssembler(inputCols=NUM + ["loai_ohe"], outputCol="features_raw", handleInvalid="error")

# 4.4 chuẩn hoá (one-hot thưa -> withMean=False)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)

pipeline = Pipeline(stages=[derive, idx, ohe, asm, scaler])
print("Pipeline stages:", [type(s).__name__ for s in pipeline.getStages()])

# ## 5. Fit thử + kiểm feature vector (chứng minh pipeline fit được)

model = pipeline.fit(feat)
out = model.transform(feat)
dim = out.select("features").first()["features"].size
print("feature vector dim:", dim, f"({len(NUM)} numeric + one-hot Loai BDS)")
out.select("price_per_m2", "features").show(3, False)

# ## 6. Ghi dataset đã join + lưu Pipeline (UNFITTED recipe)
#
# - `listings_features/sale`: dữ liệu đã as-of join macro → M7 đọc, tự fit pipeline trên train split.
# - `models/feature_pipeline`: **recipe chưa fit** → M7 fit trên train (không leakage), M8 nạp lại đúng cách biến đổi.

feat.write.mode("overwrite").parquet(FEAT_OUT)
print("ghi dataset ->", FEAT_OUT, "|", feat.count(), "dòng,", len(feat.columns), "cột")

pipeline.write().overwrite().save(PIPE_PATH)
print("lưu pipeline (unfitted) ->", PIPE_PATH)

# ## 7. Verify — nạp lại pipeline, in stages + feature dim

reloaded = Pipeline.load(PIPE_PATH)
print("nạp lại OK, stages:", [type(s).__name__ for s in reloaded.getStages()])
print("=" * 50)
print("M6 XONG:")
print(f"  dataset feature : {FEAT_OUT} ({feat.count()} dòng)")
print(f"  pipeline recipe : {PIPE_PATH}")
print(f"  feature dim     : {dim}")
print(f"  numeric         : {NUM}")
print(f"  categorical     : Loai BDS (one-hot, handleInvalid=keep)")

spark.stop()
