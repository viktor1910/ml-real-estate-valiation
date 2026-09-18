"""
M4 — Raw Parquet landing.
Đọc Kafka topic real_estate_raw → ghi Parquet thô ra data/lake/listings_raw/
phân vùng theo ngày crawl (dt=YYYY-MM-DD). Append mỗi lần chạy.

Chạy (Spark local, nhất quán với M5–M8):
    JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
        .venv/bin/python streaming/kafka_to_parquet.py

Dùng trigger(availableNow=True): đọc hết offset mới rồi dừng (không stream liên tục).
Checkpoint tại data/lake/_checkpoints/listings_raw — xóa checkpoint nếu muốn re-read từ đầu.
"""
import os
import sys

# Spark local → chạy từ project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = "real_estate_raw"
LAKE_PATH     = "data/lake/listings_raw"
CHECKPOINT    = "data/lake/_checkpoints/listings_raw"

# Tất cả cột giữ raw string — cast/parse ở M5 ETL
RAW_SCHEMA = StructType([
    StructField("ad_id",          StringType()),
    StructField("title",          StringType()),
    StructField("price",          StringType()),   # VND nguyên hoặc string — cast ở M5
    StructField("price_string",   StringType()),
    StructField("area",           StringType()),   # "60 m²" — parse ở M5
    StructField("ward",           StringType()),
    StructField("district",       StringType()),
    StructField("city",           StringType()),
    StructField("property_type",  StringType()),
    StructField("ad_type",        StringType()),   # "s"=bán, "r"=thuê
    StructField("bedrooms",       StringType()),
    StructField("bathrooms",      StringType()),
    StructField("floors",         StringType()),
    StructField("direction",      StringType()),
    StructField("interior",       StringType()),
    StructField("legal",          StringType()),
    StructField("apartment_type", StringType()),
    StructField("latitude",       DoubleType()),
    StructField("longitude",      DoubleType()),
    StructField("posted_at",      StringType()),
    StructField("url",            StringType()),
    StructField("crawled_at",     StringType()),
])


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToParquet")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
        .select("d.*")
        # Partition key: ngày crawl; lọc null tránh partition dt=null
        .filter(col("crawled_at").isNotNull())
        .withColumn("dt", to_date(col("crawled_at")))
    )

    query = (
        stream.writeStream
        .format("parquet")
        .option("path", LAKE_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("dt")
        .trigger(availableNow=True)   # Spark 3.3+; dùng once=True nếu Spark 3.2
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
    print(f"Done. Written to {LAKE_PATH}")


if __name__ == "__main__":
    main()
