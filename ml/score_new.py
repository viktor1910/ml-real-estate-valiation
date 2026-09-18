#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
score_new

Auto-converted from score_new.ipynb for deployment.
Spark job: requires JAVA_HOME set to a JDK 17 install, e.g.
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
Run:
    python3 score_new.py
"""


# # M8 — Luồng B: Score tin mới (load model, KHÔNG train lại)
#
# **Mục tiêu:** mỗi lô crawl → bảng dự đoán + danh sách top định giá thấp, không train lại.
#
# **Kiến trúc:** nạp 1 artifact `PipelineModel` (M7, đã gồm feature stages M6 + RF) →
# `transform` lô tin mới (đã ghép vĩ mô theo `posted_at`) → `prediction` = giá dự đoán (triệu/m²).
#
# **Quyết định (user chốt 2026-09-17):**
# - Lô demo = **toàn bộ `listings_clean/sale` (2156 dòng)** — chưa có crawl mới, tái dùng data hiện có.
# - **Cờ định giá thấp:** `undervalued_ratio = (pred − thực)/pred ≥ 0.20` (20%).
# - **Drift check:** RMSE lô mới vs `baseline_rmse` (M7). Cảnh báo "cần retrain" khi
#   `rmse > 1.5 × baseline` (15.128 → **22.69**).
# - ⚠️ **Trung thực:** RMSE trên full 2156 **lạc quan** (lô trùng data đã train) → drift check
#   tính RMSE **chỉ trên 381 dòng test** (`randomSplit` seed=42 y hệt M7) để có số holdout thật.
#   Bảng dự đoán vẫn xuất đủ full batch.
#
# **Chạy lại:**
# `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home .venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=python3 ml/score_new.ipynb`

import os, json, datetime
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

# --- Cấu hình (đổi NEW_BATCH khi có crawl mới) ---
NEW_BATCH  = os.getenv("SCORE_NEW_BATCH", "data/lake/listings_clean/sale")  # override qua env khi cron truyền lô crawl hôm nay
MARKET_LAKE = "data/lake/macro_raw"
MONTHLY_CSV = "data/raw_csv/macro/cpi_rate_monthly.csv"
# version model = con trỏ prod đã promote (models/current.json); fallback env/cứng
_CUR = "models/current.json"
if os.path.exists(_CUR):
    VERSION = json.load(open(_CUR, encoding="utf-8"))["version"]
else:
    VERSION = os.getenv("MODEL_VERSION", "2026-09-17")
MODEL_PATH = f"models/v{VERSION}/model"            # FULL PipelineModel (M7)
METRICS    = f"models/v{VERSION}/metrics.json"
PRED_OUT   = "data/lake/predictions/sale"          # output: bảng dự đoán (M9 đọc)
LABEL      = "price_per_m2"                          # giá thực (triệu/m²) — có trong lô để drift check

UNDERVALUED_TH = 0.20     # cờ định giá thấp khi ratio >= 20%
DRIFT_MULT     = 1.5      # cảnh báo retrain khi rmse > 1.5 * baseline
SEED           = 42       # y hệt M7 để tái tạo test split cho drift

with open(METRICS) as f:
    BASELINE_RMSE = json.load(f)["baseline_rmse"]
DRIFT_TH = DRIFT_MULT * BASELINE_RMSE
print(f"baseline_rmse={BASELINE_RMSE:.3f} | drift threshold={DRIFT_TH:.3f} (>{DRIFT_MULT}x)")

spark = (
    SparkSession.builder.appName("M8-score")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Đọc lô tin mới + GHÉP VĨ MÔ theo posted_at (y hệt M6 qua macro_features)
from macro_features import attach_macro

listings = spark.read.parquet(NEW_BATCH)
print("lô mới:", listings.count(), "dòng,", len(listings.columns), "cột")

feat, gate_on = attach_macro(spark, listings, MARKET_LAKE, MONTHLY_CSV)
print(f"macro gate_on={gate_on}")

# 2) Nạp FULL PipelineModel + transform → prediction (giá dự đoán triệu/m²)
model = PipelineModel.load(MODEL_PATH)
print("nạp model:", [type(s).__name__ for s in model.stages])

scored = model.transform(feat)

# 3) undervalued_ratio + cờ. prediction = giá/m² dự đoán; LABEL = giá/m² thực
scored = (
    scored
    .withColumnRenamed("prediction", "predicted_ppm2")
    .withColumn("undervalued_ratio",
                (F.col("predicted_ppm2") - F.col(LABEL)) / F.col("predicted_ppm2"))
    .withColumn("is_undervalued", F.col("undervalued_ratio") >= F.lit(UNDERVALUED_TH))
    # giá tổng (VND) cho dễ đọc: giá/m² (triệu) * diện tích * 1e6
    .withColumn("predicted_total_vnd",
                F.round(F.col("predicted_ppm2") * F.col("area") * 1e6).cast("long"))
)
n_scored = scored.count()
n_under  = scored.filter("is_undervalued").count()
print(f"đã chấm {n_scored} tin | định giá thấp (>={int(UNDERVALUED_TH*100)}%): {n_under} "
      f"({100*n_under/n_scored:.1f}%)")

# 4) DRIFT CHECK — RMSE chỉ trên 381 dòng test (randomSplit seed=42 y hệt M7)
#    (full batch trùng data train -> RMSE lạc quan; test split = holdout thật)
_, test = feat.randomSplit([0.8, 0.2], seed=SEED)
test_pred = model.transform(test)
ev = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="rmse")
batch_rmse = ev.evaluate(test_pred)

# RMSE trên full batch (tham khảo, sẽ thấp hơn vì gồm cả train)
full_rmse = RegressionEvaluator(
    labelCol=LABEL, predictionCol="predicted_ppm2", metricName="rmse"
).evaluate(scored)

drift = batch_rmse > DRIFT_TH
print(f"RMSE test-holdout = {batch_rmse:.3f} | baseline = {BASELINE_RMSE:.3f} "
      f"| ngưỡng drift = {DRIFT_TH:.3f}")
print(f"RMSE full-batch   = {full_rmse:.3f} (lạc quan — gồm data train)")
if drift:
    print(f"🔴 DRIFT: RMSE {batch_rmse:.3f} > {DRIFT_TH:.3f} → CẦN RETRAIN (chạy lại M7)")
else:
    print(f"🟢 OK: RMSE {batch_rmse:.3f} <= {DRIFT_TH:.3f} → không cần retrain")

# 4b) GHI CỜ DRIFT — cron/orchestrator đọc file này để quyết định có retrain không.
#     Tách tín hiệu khỏi hành động: score chỉ báo, không tự train.
FLAG_OUT = os.getenv("RETRAIN_FLAG", "models/retrain_needed.json")
with open(FLAG_OUT, "w", encoding="utf-8") as f:
    json.dump({
        "drift": bool(drift),
        "batch_rmse": round(batch_rmse, 3),
        "baseline_rmse": round(BASELINE_RMSE, 3),
        "drift_threshold": round(DRIFT_TH, 3),
        "model_version": VERSION,
        "checked_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }, f, ensure_ascii=False, indent=2)
print(f"cờ drift -> {FLAG_OUT} (drift={drift})")

# 5) Bảng dự đoán (cột cho M9) + ghi parquet + top định giá thấp
SCORED_AT = datetime.datetime.now().isoformat(timespec="seconds")
preds = (
    scored.select(
        F.col("ad_id").alias("id"),
        F.col("district").alias("district"),
        F.col("area").alias("area_m2"),
        F.round(F.col(LABEL), 2).alias("listing_ppm2"),          # giá thực (triệu/m²)
        F.round(F.col("predicted_ppm2"), 2).alias("predicted_ppm2"),
        F.col("predicted_total_vnd"),
        F.round(F.col("undervalued_ratio"), 4).alias("undervalued_ratio"),
        F.col("is_undervalued"),
        F.col("url"),
        F.lit(VERSION).alias("model_version"),
        F.lit(SCORED_AT).alias("scored_at"),
    )
)
preds.write.mode("overwrite").parquet(PRED_OUT)
print(f"ghi bảng dự đoán -> {PRED_OUT} | {preds.count()} dòng")

print("\n=== TOP 15 ĐỊNH GIÁ THẤP (undervalued_ratio giảm dần) ===")
(preds.filter("is_undervalued")
      .orderBy(F.col("undervalued_ratio").desc())
      .select("district", "area_m2", "listing_ppm2", "predicted_ppm2",
              "undervalued_ratio", "predicted_total_vnd", "url")
      .show(15, truncate=60))

print("=== Số tin định giá thấp theo quận ===")
(preds.filter("is_undervalued").groupBy("district").count()
      .orderBy(F.col("count").desc()).show(25, truncate=False))

print("=" * 55)
print("M8 XONG:")
print(f"  lô chấm         : {NEW_BATCH} ({n_scored} tin)")
print(f"  định giá thấp   : {n_under} tin (>={int(UNDERVALUED_TH*100)}%)")
print(f"  drift (test)    : RMSE={batch_rmse:.3f} vs baseline={BASELINE_RMSE:.3f} "
      f"-> {'RETRAIN' if drift else 'OK'}")
print(f"  bảng dự đoán    : {PRED_OUT}")
spark.stop()
