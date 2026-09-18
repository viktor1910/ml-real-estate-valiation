#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_predictions — M9. Nạp bảng dự đoán (PRED_OUT parquet) -> Postgres (serving).

Tách khỏi score_new: score chỉ chấm + ghi parquet; bước này đẩy vào DB để phục vụ.
Ghi **APPEND** — giữ lịch sử theo `scored_at` + `model_version` (init.sql chưa có PK
nên không dedup; mỗi lần chạy cộng thêm lô mới).

Kết nối lấy từ env, mặc định khớp docker-compose.yml:
    PG_HOST=localhost PG_PORT=5432 PG_DB=realestate PG_USER=admin PG_PASSWORD=admin

Cần JAVA_HOME (JDK17) như các Spark job khác. Postgres JDBC jar tự tải qua
`spark.jars.packages` (org.postgresql:postgresql:42.7.4) — cần mạng lần chạy đầu.

Chạy:
    JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
        .venv/bin/python ml/load_predictions.py
"""

import os
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

from pyspark.sql import SparkSession, functions as F

# --- Cấu hình (env override; mặc định = docker-compose.yml) ---
PRED_OUT      = os.getenv("PRED_OUT", "data/lake/predictions/sale")  # nguồn: output score_new
PG_HOST       = os.getenv("PG_HOST", "localhost")
PG_PORT       = os.getenv("PG_PORT", "5432")
PG_DB         = os.getenv("PG_DB", "realestate")
PG_USER       = os.getenv("PG_USER", "admin")
PG_PASSWORD   = os.getenv("PG_PASSWORD", "admin")
PG_TABLE      = os.getenv("PG_TABLE", "predictions")           # đã tạo sẵn bởi sql/init.sql
PG_DRIVER_PKG = os.getenv("PG_DRIVER_PKG", "org.postgresql:postgresql:42.7.4")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

spark = (
    SparkSession.builder.appName("M9-load-predictions")
    .master("local[*]")
    .config("spark.jars.packages", PG_DRIVER_PKG)   # tự tải JDBC jar (ivy)
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Đọc parquet + cast type khớp init.sql (id BIGINT, scored_at TIMESTAMP).
#    Các cột còn lại (double/long/bool/text) đã khớp sẵn schema bảng predictions.
preds = spark.read.parquet(PRED_OUT)
n = preds.count()
preds = (
    preds
    .withColumn("id", F.col("id").cast("long"))          # ad_id string -> BIGINT
    .withColumn("scored_at", F.to_timestamp("scored_at"))  # ISO string -> TIMESTAMP
)

# 2) APPEND -> Postgres. Bảng đã tồn tại (init.sql) nên chỉ chèn, không tạo lại.
(preds.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save())

print(f"✅ nạp {n} dòng -> {JDBC_URL} bảng {PG_TABLE} (append)")
spark.stop()
