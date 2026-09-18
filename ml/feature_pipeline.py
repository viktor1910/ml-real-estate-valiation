#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feature_pipeline — M6. As-of join vĩ mô + build Spark ML Pipeline recipe. Schema English.

Đọc listings_clean/sale → as-of join macro theo posted_at → ghi listings_features/sale
(M7 train đọc). Lưu Pipeline recipe (unfitted) — M7 fit trên train split.

Categorical: property_type + interior (StringIndexer -> OneHot). Numeric bỏ bathrooms.
"""

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
    SQLTransformer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
)

CLEAN     = "data/lake/listings_clean/sale"
MACRO     = "data/raw_csv/macro/macro_daily.csv"
FEAT_OUT  = "data/lake/listings_features/sale"
PIPE_PATH = "models/feature_pipeline"
HCM_LAT, HCM_LON = 10.7769, 106.7009

spark = (
    SparkSession.builder.appName("M6-feature-pipeline")
    .master("local[*]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

listings = spark.read.parquet(CLEAN)
print("listings_clean:", listings.count(), "dòng,", len(listings.columns), "cột")

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

# as-of join theo posted_at (macro_daily đã forward-fill lịch ngày liên tục)
listings = listings.withColumn("posted_date", F.to_date("posted_at"))
feat = listings.join(macro, listings["posted_date"] == macro["d"], "left").drop("d")

miss = feat.filter(F.col("vnindex").isNull() | F.col("gold_usd").isNull() | F.col("usdvnd").isNull()).count()
pr = feat.select(F.min("posted_date"), F.max("posted_date")).first()
print(f"posted_date {pr[0]} -> {pr[1]} | dòng thiếu macro: {miss}")
if miss:
    # as-of carry-forward: post sau ngày macro cuối (vd hôm nay) -> dùng phiên macro gần nhất đã biết
    last = macro.orderBy(F.col("d").desc()).first()
    feat = feat.fillna({"gold_usd": last["gold_usd"], "usdvnd": last["usdvnd"], "vnindex": last["vnindex"]})
    print(f"  -> carry-forward {miss} dòng bằng macro ngày {last['d']} (gold={last['gold_usd']}, vnindex={last['vnindex']})")

# --- Spark ML Pipeline recipe (English) ---
derive = SQLTransformer(statement=f"""
    SELECT *,
        log(area)                                     AS log_area,
        year(posted_at)                               AS year,
        month(posted_at)                              AS month,
        quarter(posted_at)                            AS quarter,
        dayofweek(posted_at)                          AS dayofweek,
        6371 * 2 * asin(sqrt(
            power(sin(radians(latitude - {HCM_LAT}) / 2), 2) +
            cos(radians({HCM_LAT})) * cos(radians(latitude)) *
            power(sin(radians(longitude - {HCM_LON}) / 2), 2)
        ))                                            AS dist_center,
        coalesce(property_type, 'UNKNOWN')            AS property_type_s,
        coalesce(interior, 'UNKNOWN')                 AS interior_s
    FROM __THIS__
""")

idx_pt  = StringIndexer(inputCol="property_type_s", outputCol="pt_idx", handleInvalid="keep")
ohe_pt  = OneHotEncoder(inputCol="pt_idx", outputCol="pt_ohe", handleInvalid="keep")
idx_int = StringIndexer(inputCol="interior_s", outputCol="int_idx", handleInvalid="keep")
ohe_int = OneHotEncoder(inputCol="int_idx", outputCol="int_ohe", handleInvalid="keep")

NUM = [
    "log_area", "bedrooms", "floors", "rank_quan", "dist_center",
    "gold_usd", "usdvnd", "vnindex",
    "year", "month", "quarter", "dayofweek",
]
asm = VectorAssembler(inputCols=NUM + ["pt_ohe", "int_ohe"], outputCol="features_raw", handleInvalid="error")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)

pipeline = Pipeline(stages=[derive, idx_pt, ohe_pt, idx_int, ohe_int, asm, scaler])
print("Pipeline stages:", [type(s).__name__ for s in pipeline.getStages()])

model = pipeline.fit(feat)
out = model.transform(feat)
dim = out.select("features").first()["features"].size
print("feature vector dim:", dim)

feat.write.mode("overwrite").parquet(FEAT_OUT)
print("ghi dataset ->", FEAT_OUT, "|", feat.count(), "dòng,", len(feat.columns), "cột")
pipeline.write().overwrite().save(PIPE_PATH)
print("lưu pipeline (unfitted) ->", PIPE_PATH)
print("M6 XONG | feature dim:", dim, "| numeric:", NUM)
spark.stop()
