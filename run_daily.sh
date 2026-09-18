#!/usr/bin/env bash
# run_daily — Piece 4. Vòng lặp hằng ngày (cron). Nối cả pipeline:
#   crawl -> Kafka -> raw parquet -> clean_parse(pool) -> impute serve -> score(+cờ drift)
#   -> NẾU drift: retrain + promote-if-better.
#
# Cron ví dụ (2h sáng mỗi ngày):
#   0 2 * * *  /Users/viktornguyen/Desktop/viktor/Machine\ learning/run_daily.sh >> /tmp/daily.log 2>&1
set -euo pipefail
cd "$(dirname "$0")"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
PY="${PY:-.venv/bin/python}"
DT="$(date +%F)"

echo "===== DAILY $DT ====="

# 1) crawl -> Kafka  (bật khi Kafka chạy; demo dùng replay_producer thay spider)
# (cd scraper && scrapy crawl chotot)
# $PY streaming/replay_producer.py --csv data/raw_csv/chotot_bds_data.csv

# 2) Kafka -> raw parquet landing (tích lũy theo dt)
# $PY streaming/kafka_to_parquet.py

# 3) clean_parse: raw -> pool (giữ null), append partition dt=hôm nay
$PY ml/clean_parse.py --src data/lake/listings_raw --dt "$DT"

# 4) impute serve (stats đóng băng): lô hôm nay -> scored_input (giữ mọi tin)
$PY ml/impute_apply.py --src "data/lake/listings_pool/dt=$DT" --out data/lake/scored_input --mode serve

# 5) score lô hôm nay + ghi cờ drift
SCORE_NEW_BATCH=data/lake/scored_input $PY ml/score_new.py

# 6) đọc cờ -> nếu drift thì retrain + promote
DRIFT="$($PY -c 'import json;print(json.load(open("models/retrain_needed.json"))["drift"])')"
if [ "$DRIFT" = "True" ]; then
  echo "🔴 DRIFT → chạy retrain"
  bash ml/retrain.sh
else
  echo "🟢 không drift → giữ model hiện tại"
fi

echo "===== DAILY XONG ====="
