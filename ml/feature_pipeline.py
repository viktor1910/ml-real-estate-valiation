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
from config import build_spark, LAKE, MODEL_STORE, PG_DRIVER_PKG

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    SQLTransformer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
)

CLEAN     = f"{LAKE}/listings_clean/sale"
MARKET_LAKE = f"{LAKE}/macro_raw"
MONTHLY_TABLE = os.getenv("PG_MACRO_TABLE", "macro_monthly")  # CPI/lãi suất -> Postgres
FEAT_OUT  = f"{LAKE}/listings_features/sale"
PIPE_PATH = f"{MODEL_STORE}/feature_pipeline"
HCM_LAT, HCM_LON = 10.7769, 106.7009

spark = build_spark("M6-feature-pipeline", packages=PG_DRIVER_PKG)

from macro_features import attach_macro, MACRO_COLS

listings = spark.read.parquet(CLEAN)
print("listings_clean:", listings.count(), "dòng,", len(listings.columns), "cột")

# Gate + as-of join macro (trailing lag/rolling). Gate OFF -> 0 cột macro.
feat, gate_on = attach_macro(spark, listings, MARKET_LAKE, MONTHLY_TABLE)
macro_present = [c for c in MACRO_COLS if c in feat.columns]
print(f"macro gate_on={gate_on} | {len(macro_present)} cột macro: {macro_present}")

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

NUM_BASE = [
    "log_area", "bedrooms", "floors", "rank_quan", "dist_center",
    "year", "month", "quarter", "dayofweek",
]
NUM = NUM_BASE + macro_present   # macro chỉ vào khi gate ON
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
