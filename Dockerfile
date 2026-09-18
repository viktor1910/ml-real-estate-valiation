# ============================================================================
# Real Estate Valuation — image DUY NHẤT cho mọi vai trò Spark:
#   spark-master, spark-worker, và ml-app (driver chạy pipeline).
# Không python UDF trong pipeline -> worker chạy JVM thuần, nhưng dùng chung
# 1 image cho đơn giản vận hành.
#
# Python 3.11 (KHÔNG 3.14 như .venv dev) — ổn định + pyspark 4.2 hỗ trợ tốt.
# ============================================================================
FROM python:3.11-slim-bookworm

# --- JDK 17 (Spark cần) + tiện ích mạng cho healthcheck ---
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# --- Python deps (pyspark mang theo bản Spark bundled) ---
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && pip install --no-cache-dir -r requirements.txt

# SPARK_HOME = nơi pip đặt pyspark; thêm sbin/bin vào PATH cho spark-class.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# --- Pre-warm ivy cache: tải sẵn jar s3a/kafka/jdbc lúc build (offline khi chạy).
#     Version PHẢI khớp Hadoop bundle của pyspark. Build FAIL LOUD nếu sai version.
ARG SPARK_JARS_PACKAGES=org.apache.hadoop:hadoop-aws:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.4
ENV WARM_PKGS=${SPARK_JARS_PACKAGES}
RUN python - <<'PY'
import os
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("ivy-warmup").master("local[1]")
     .config("spark.jars.packages", os.environ["WARM_PKGS"]).getOrCreate())
print("ivy warmup OK:", os.environ["WARM_PKGS"])
s.stop()
PY

# --- Mã nguồn ứng dụng ---
COPY ml/ ./ml/
COPY streaming/ ./streaming/
COPY scraper/ ./scraper/
COPY sql/ ./sql/
COPY run_daily.sh ./
RUN chmod +x run_daily.sh ml/retrain.sh || true

# Scripts dùng `$PY` — trong container = system python (pyspark cài ở đây, không .venv).
ENV PY=python

# META_DIR mặc định (JSON điều khiển driver-local) — mount volume đè khi cần.
RUN mkdir -p /app/models_meta /app/logs

CMD ["bash"]
