#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train

Auto-converted from train.ipynb for deployment.
Spark job: requires JAVA_HOME set to a JDK 17 install, e.g.
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
Run:
    python3 train.py
"""


# # M7 — Luồng A: Train + tuning + lưu model
#
# Đọc `listings_features/sale` (M6, macro đã as-of join) → **chia train/test** → thử 3 regressor (LinearRegression, RandomForest, GBT) với `CrossValidator` + `ParamGridBuilder` → **ablation vĩ mô** (có vs không macro) → lưu **PipelineModel đã fit** có version + bảng chỉ số.
#
# **Quyết định split (user 2026-09-17):** dùng **random 80/20 (seed=42)**, KHÔNG chia theo năm 2025/2026.
# > Lý do: data 2142/2156 dòng dồn Dec-2025, chỉ 14 dòng 2026 → chia theo năm cho test n=14 (vô nghĩa thống kê). Random split cho test ~431 dòng, chỉ số ổn định. Đánh đổi: lệch chủ ý chia-theo-thời-gian của spec M7 — chấp nhận vì toàn bộ data gần như cùng một cửa sổ thời gian nên leakage thời gian không đáng kể.
#
# **Target:** `price_per_m2` (triệu/m²).
#
# **Model lưu = FULL PipelineModel** (feature stages fit trên train + regressor) → M8 nạp 1 artifact, `transform` tin mới (đã as-of join macro) → prediction. Tránh lệch train/serve.

# ## 1. Config + Java + imports

import os, json, datetime
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
from config import build_spark, LAKE, MODEL_STORE, META_DIR

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    SQLTransformer, StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler,
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

FEAT_IN   = f"{LAKE}/listings_features/sale"   # input (M6)
LABEL     = "price_per_m2"
VERSION   = datetime.date.today().isoformat()    # vd 2026-09-17
MODEL_DIR = f"{MODEL_STORE}/v{VERSION}"          # binary model (Spark, có thể s3a)
META_VDIR = f"{META_DIR}/v{VERSION}"             # metrics.json (driver-local)
SEED      = 42

# ## 2. Spark + đọc feature dataset + chia train/test (random 80/20)

spark = build_spark("M7-train")

feat = spark.read.parquet(FEAT_IN)
train, test = feat.randomSplit([0.8, 0.2], seed=SEED)
train.cache(); test.cache()
n_train, n_test = train.count(), test.count()
print(f"tổng {feat.count()} | train {n_train} | test {n_test}")

# ## 3. Feature stages (tái dựng M6) — biến thể có macro / không macro
#
# Ablation vĩ mô = build 2 bộ đặc trưng giống hệt nhau, chỉ khác việc có đưa `gold_usd/usdvnd/vnindex` vào `VectorAssembler` hay không. Mọi biến đổi per-row nằm trong pipeline → M8 tái dựng y hệt.

# SQLTransformer y hệt M6 (cột phái sinh, schema English)
DERIVE_SQL = """
    SELECT *,
        log(area)                                     AS log_area,
        year(posted_at)                               AS year,
        month(posted_at)                              AS month,
        quarter(posted_at)                            AS quarter,
        dayofweek(posted_at)                          AS dayofweek,
        6371 * 2 * asin(sqrt(
            power(sin(radians(latitude - 10.7769) / 2), 2) +
            cos(radians(10.7769)) * cos(radians(latitude)) *
            power(sin(radians(longitude - 106.7009) / 2), 2)
        ))                                            AS dist_center,
        coalesce(property_type, 'UNKNOWN')            AS property_type_s,
        coalesce(interior, 'UNKNOWN')                 AS interior_s
    FROM __THIS__
"""

NUM_BASE = [
    "log_area", "bedrooms", "floors", "rank_quan", "dist_center",
    "year", "month", "quarter", "dayofweek",
]
from macro_features import MACRO_COLS
# chỉ dùng cột macro thực sự có trong dataset (gate có thể đã drop hết)
MACRO = [c for c in MACRO_COLS if c in feat.columns]
print(f"train: {len(MACRO)} cột macro có trong features")
if not MACRO:
    print("train: gate OFF -> ablation macro/no_macro trùng nhau (delta_rmse≈0), đúng thiết kế")

def feature_stages(use_macro: bool):
    """Stage đặc trưng M6 (English); toggle 3 cột macro. Categorical: property_type + interior."""
    derive = SQLTransformer(statement=DERIVE_SQL)
    idx_pt  = StringIndexer(inputCol="property_type_s", outputCol="pt_idx", handleInvalid="keep")
    ohe_pt  = OneHotEncoder(inputCol="pt_idx", outputCol="pt_ohe", handleInvalid="keep")
    idx_int = StringIndexer(inputCol="interior_s", outputCol="int_idx", handleInvalid="keep")
    ohe_int = OneHotEncoder(inputCol="int_idx", outputCol="int_ohe", handleInvalid="keep")
    num = NUM_BASE + (MACRO if use_macro else [])
    asm = VectorAssembler(inputCols=num + ["pt_ohe", "int_ohe"], outputCol="features_raw",
                          handleInvalid="error")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                            withStd=True, withMean=False)
    return [derive, idx_pt, ohe_pt, idx_int, ohe_int, asm, scaler]

# ## 4. Regressor + lưới tham số (tuning)
#
# `CrossValidator` 3-fold chọn tham số theo RMSE trên train. Lưới nhỏ gọn (đủ minh họa "cách điều chỉnh tham số" cho báo cáo, không đốt compute).

def build_cases():
    """(tên, estimator, paramGrid-builder-fn theo estimator)."""
    def lr_grid(m):
        return (ParamGridBuilder()
                .addGrid(m.regParam, [0.0, 0.1])
                .addGrid(m.elasticNetParam, [0.0, 0.5]).build())
    def rf_grid(m):
        return (ParamGridBuilder()
                .addGrid(m.numTrees, [50, 100])
                .addGrid(m.maxDepth, [5, 10]).build())
    def gbt_grid(m):
        return (ParamGridBuilder()
                .addGrid(m.maxDepth, [3, 5])
                .addGrid(m.maxIter, [30, 50]).build())
    return [
        ("LinearRegression", LinearRegression(featuresCol="features", labelCol=LABEL), lr_grid),
        ("RandomForest",     RandomForestRegressor(featuresCol="features", labelCol=LABEL, seed=SEED), rf_grid),
        ("GBT",              GBTRegressor(featuresCol="features", labelCol=LABEL, seed=SEED), gbt_grid),
    ]

ev_rmse = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="rmse")
ev_mae  = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="mae")
ev_r2   = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="r2")

# ## 5. Train + tune tất cả (model × có/không macro)
#
# Mỗi ô: `CrossValidator` fit **toàn bộ pipeline** (feature stages fit CHỈ trên train fold → scaler/indexer không leakage) → best → đánh giá trên test.

results = []
best_models = {}   # key -> bestModel (full PipelineModel)

for use_macro in (True, False):
    tag = "macro" if use_macro else "no_macro"
    fstages = feature_stages(use_macro)
    for name, est, grid_fn in build_cases():
        pipe = Pipeline(stages=fstages + [est])
        cv = CrossValidator(
            estimator=pipe,
            estimatorParamMaps=grid_fn(est),
            evaluator=ev_rmse,
            numFolds=3, seed=SEED, parallelism=2,
        )
        cv_model = cv.fit(train)
        best = cv_model.bestModel
        pred = best.transform(test)
        row = {
            "model": name, "features": tag,
            "rmse": ev_rmse.evaluate(pred),
            "mae":  ev_mae.evaluate(pred),
            "r2":   ev_r2.evaluate(pred),
            "cv_rmse_train": min(cv_model.avgMetrics),
        }
        results.append(row)
        best_models[(name, tag)] = best
        print(f"[{tag:8}] {name:16} test RMSE={row['rmse']:.3f} MAE={row['mae']:.3f} R2={row['r2']:.3f}")

# ## 6. Bảng chỉ số + ablation vĩ mô

res = pd.DataFrame(results).sort_values(["features", "rmse"]).reset_index(drop=True)
print("=== Bảng chỉ số (test) — target price_per_m2 (triệu/m²) ===")
print(res.to_string(index=False))

print("\n=== Ablation vĩ mô (cùng model, macro − no_macro; RMSE âm = macro tốt hơn) ===")
piv = res.pivot(index="model", columns="features", values="rmse")
piv["delta_rmse"] = piv["macro"] - piv["no_macro"]
print(piv.to_string())
print("\nGhi chú: macro biến thiên nhỏ (data dồn ~11 ngày Dec-2025) → cải thiện dự kiến khiêm tốn; báo cáo trung thực theo số.")

# ## 7. Chọn model tốt nhất (có macro) + lưu có version
#
# Model sản phẩm dùng **có macro** (đúng kiến trúc Luồng B ghép vĩ mô). Chọn RMSE test nhỏ nhất trong nhóm macro. Lưu full `PipelineModel` + `metrics.json` (baseline RMSE cho drift check M8).

macro_rows = [r for r in results if r["features"] == "macro"]
best_row = min(macro_rows, key=lambda r: r["rmse"])
best_key = (best_row["model"], "macro")
best_model = best_models[best_key]

os.makedirs(META_VDIR, exist_ok=True)   # metrics.json ghi bằng open() -> cần dir local
best_model.write().overwrite().save(f"{MODEL_DIR}/model")

metrics = {
    "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
    "version": VERSION,
    "target": LABEL,
    "split": {"method": "random", "ratio": [0.8, 0.2], "seed": SEED,
              "n_train": n_train, "n_test": n_test},
    "results": results,
    "best": {"model": best_row["model"], "features": "macro",
             "rmse": best_row["rmse"], "mae": best_row["mae"], "r2": best_row["r2"]},
    "baseline_rmse": best_row["rmse"],   # M8 drift check so với số này
}
with open(f"{META_VDIR}/metrics.json", "w") as f:
    json.dump(metrics, f, ensure_ascii=False, indent=2)

print(f"model tốt nhất (macro): {best_row['model']} | test RMSE={best_row['rmse']:.3f} R2={best_row['r2']:.3f}")
print(f"lưu -> {MODEL_DIR}/model")
print(f"metrics -> {META_VDIR}/metrics.json | baseline_rmse={metrics['baseline_rmse']:.3f}")

# ## 8. Verify — nạp lại model + predict thử

from pyspark.ml import PipelineModel
reloaded = PipelineModel.load(f"{MODEL_DIR}/model")
sample = reloaded.transform(test).select(LABEL, "prediction").limit(5)
print("nạp lại OK. Mẫu (thực vs dự đoán, triệu/m²):")
sample.show(truncate=False)
print("=" * 55)
print("M7 XONG:")
print(f"  split      : random 80/20 seed={SEED} (train {n_train} / test {n_test})")
print(f"  best model : {best_row['model']} (macro) RMSE={best_row['rmse']:.3f} R2={best_row['r2']:.3f}")
print(f"  path       : {MODEL_DIR}/model")
spark.stop()
