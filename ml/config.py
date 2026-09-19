#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config — nguồn cấu hình DUY NHẤT cho mọi Spark job (deploy phân tán).

Mục tiêu: KHÔNG hardcode path/JAVA_HOME/master trong từng file nữa. Mọi thứ
đọc từ biến môi trường; **mặc định giữ nguyên hành vi chạy local cũ** để dev
trên máy không đổi gì.

Phân tầng lưu trữ (xem DEPLOY.md):
  LAKE        — parquet (Spark đọc/ghi, executor truy cập)  -> prod: s3a://.../lake
  MODEL_STORE — PipelineModel + feature_pipeline (Spark)     -> prod: s3a://.../models
  META_DIR    — JSON nhỏ (driver đọc bằng open(), KHÔNG s3a) -> prod: volume local
  RAW_CSV     — CSV seed tĩnh (mount)                        -> prod: bind mount

build_spark(): tạo SparkSession với master + (nếu dùng s3a) cấu hình MinIO/S3.
Không có python UDF trong pipeline -> executor chạy JVM thuần, không cần python.
"""
import os

# --- Roots (env override; default = hành vi local cũ) ---
LAKE        = os.getenv("LAKE_ROOT", "data/lake")
MODEL_STORE = os.getenv("MODEL_STORE", "models")
META_DIR    = os.getenv("META_DIR", "models")
RAW_CSV     = os.getenv("RAW_CSV_ROOT", "data/raw_csv")

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SHUFFLE_PARTS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

# --- Postgres (serving M9 + macro store); default = docker-compose.yml ---
# Nguồn DUY NHẤT cho mọi job chạm Postgres (load_predictions, macro read).
PG_HOST       = os.getenv("PG_HOST", "localhost")
PG_PORT       = os.getenv("PG_PORT", "5432")
PG_DB         = os.getenv("PG_DB", "realestate")
PG_USER       = os.getenv("PG_USER", "admin")
PG_PASSWORD   = os.getenv("PG_PASSWORD", "admin")
PG_DRIVER_PKG = os.getenv("PG_DRIVER_PKG", "org.postgresql:postgresql:42.7.4")
JDBC_URL      = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def pg_read(spark, table):
    """Đọc 1 bảng Postgres -> Spark DataFrame (JDBC).

    Yêu cầu SparkSession tạo với build_spark(packages=PG_DRIVER_PKG) để có
    JDBC jar. `table` là tên bảng (hoặc subquery bọc ngoặc).
    """
    return (spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", table)
            .option("user", PG_USER)
            .option("password", PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load())

# JAVA_HOME: prod (container/Linux) LUÔN set env này -> setdefault không đè.
# Fallback chỉ dùng khi chạy local mac chưa export. Trên Linux nếu chưa set,
# thử vị trí JDK phổ biến của Debian/Ubuntu (khớp Dockerfile).
_JAVA_FALLBACK = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    if os.path.isdir("/opt/homebrew/opt/openjdk@17")
    else "/usr/lib/jvm/java-17-openjdk-amd64"
)
os.environ.setdefault("JAVA_HOME", _JAVA_FALLBACK)


def _is_s3(*paths) -> bool:
    return any(str(p).startswith("s3a://") for p in paths)


def _s3_conf(builder):
    """Cấu hình S3A trỏ MinIO/S3. Đọc từ env chuẩn AWS + S3_ENDPOINT."""
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access   = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret   = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    ssl      = os.getenv("S3_SSL_ENABLED", "false")
    return (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl)
        # Đệm upload bằng RAM (off-heap) thay đĩa -> tránh lỗi buffer.dir capacity 0
        # trên worker container. Data nhỏ nên an toàn.
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )


def build_spark(app_name: str, packages: str = ""):
    """
    SparkSession chuẩn cho mọi job.
      - master: env SPARK_MASTER (default local[*] -> dev không đổi).
      - s3a: tự bật khi LAKE/MODEL_STORE là s3a:// (jar hadoop-aws bake sẵn trong image).
      - packages: chỉ set khi cần tải jar qua ivy (vd JDBC lúc chưa bake) — prod để rỗng.
    """
    from pyspark.sql import SparkSession

    b = (SparkSession.builder
         .appName(app_name)
         .master(SPARK_MASTER)
         .config("spark.sql.shuffle.partitions", SHUFFLE_PARTS))

    pkgs = packages or os.getenv("SPARK_JARS_PACKAGES", "")
    if pkgs:
        b = b.config("spark.jars.packages", pkgs)

    if _is_s3(LAKE, MODEL_STORE, RAW_CSV):
        b = _s3_conf(b)

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
