"""
Đọc CSV cũ (14 cột tiếng Việt) → map sang schema 22 cột → gửi Kafka.
Dùng khi chưa crawl thật hoặc demo pipeline.

Chạy:
    python streaming/replay_producer.py --csv data/raw_csv/chotot_bds_data.csv
    python streaming/replay_producer.py --csv data/raw_csv/chotot_bds_data.csv --delay 0.01
"""
import argparse
import csv
import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, KafkaConnectionError

# Map tên cột CSV → tên field schema mới
COL_MAP = {
    "STT":              "ad_id",
    "Tieu de":          "title",
    "Gia (VND)":        "price",       # số nguyên VND — giữ nguyên string
    "Dien tich":        "area",        # "42 m²" — parse ở ETL
    "Dia chi":          "ward",
    "Quan/Huyen":       "district",
    "Thanh pho":        "city",
    "Loai BDS":         "property_type",
    "Phong ngu":        "bedrooms",
    "So tang":          "floors",
    "Huong":            "direction",
    "Ngay dang":        "posted_at",
    "URL":              "url",
    "Thoi gian crawl":  "crawled_at",
}

# Cột có trong schema mới nhưng không có trong CSV cũ → điền None
NEW_COLS = ("bathrooms", "interior", "legal", "apartment_type",
            "latitude", "longitude", "ad_type", "price_string")


def build_producer(bootstrap: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            retries=3,
        )
    except (BrokerNotAvailableError, KafkaConnectionError, Exception) as e:
        print(f"ERROR: Kafka không kết nối được tại {bootstrap}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Replay CSV → Kafka")
    parser.add_argument("--csv",       required=True,           help="Đường dẫn file CSV gốc")
    parser.add_argument("--topic",     default="real_estate_raw")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--delay",     type=float, default=0.05, help="Giây giữa mỗi message")
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)
    sent = 0
    skipped = 0

    with open(args.csv, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = {COL_MAP.get(k, k): (v if v != "" else None) for k, v in row.items()}
            for col in NEW_COLS:
                msg.setdefault(col, None)
            producer.send(args.topic, msg)
            sent += 1
            if args.delay > 0:
                time.sleep(args.delay)

    producer.flush()
    producer.close()
    print(f"Done — sent {sent} messages to topic '{args.topic}' (skipped {skipped})")


if __name__ == "__main__":
    main()
