#!/usr/bin/env bash
# run_daily — Piece 4. Vòng lặp hằng ngày. Nối cả pipeline:
#   crawl -> Kafka -> raw parquet -> clean_parse(pool) -> impute serve -> score(+cờ drift)
#   -> NẾU drift: retrain + promote-if-better -> nạp Postgres.
#
# PROD (container): host cron gọi
#   0 2 * * *  cd /opt/realestate && docker compose run --rm ml-app bash run_daily.sh >> logs/daily.log 2>&1
# LOCAL dev: chạy trực tiếp (dùng .venv, master local[*] — mọi biến root mặc định data/lake, models).
set -euo pipefail
cd "$(dirname "$0")"

# JAVA_HOME: container/.env đã set -> fallback chỉ cho dev bare-metal.
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
PY="${PY:-.venv/bin/python}"          # container: PY=python (đặt trong image)
DT="$(date +%F)"

# Roots (khớp ml/config.py). Prod override qua .env; default = hành vi local cũ.
LAKE="${LAKE_ROOT:-data/lake}"
META="${META_DIR:-models}"
REPLAY_CSV="${REPLAY_CSV:-data/raw_csv/chotot_bds_data.csv}"

echo "===== DAILY $DT (LAKE=$LAKE) ====="

# 0) fetch macro market series (vnstock) -> macro_raw lake (trước clean_parse)
$PY ml/fetch_macro.py || echo "⚠ fetch_macro lỗi -> dùng carry-forward"

# 1) nguồn tin -> Kafka. Mặc định replay CSV seed (ổn định cho daily/demo).
#    Bật crawl thật khi spider đã verify: (cd scraper && scrapy crawl chotot ...)
$PY streaming/replay_producer.py --csv "$REPLAY_CSV" || echo "⚠ replay_producer lỗi (Kafka down?)"

# 2) Kafka -> raw parquet landing (availableNow: đọc offset mới rồi dừng; checkpoint tích lũy)
$PY streaming/kafka_to_parquet.py

# 3) clean_parse: raw -> pool (giữ null), append partition dt=hôm nay
$PY ml/clean_parse.py --src "$LAKE/listings_raw" --dt "$DT"

# 4) impute serve (stats đóng băng): lô hôm nay -> scored_input (giữ mọi tin)
$PY ml/impute_apply.py --src "$LAKE/listings_pool/dt=$DT" --out "$LAKE/scored_input" --mode serve

# 5) score lô hôm nay + ghi cờ drift
SCORE_NEW_BATCH="$LAKE/scored_input" $PY ml/score_new.py

# 6) đọc cờ -> nếu drift thì retrain + promote
DRIFT="$($PY -c "import json;print(json.load(open('$META/retrain_needed.json'))['drift'])")"
if [ "$DRIFT" = "True" ]; then
  echo "🔴 DRIFT → chạy retrain"
  bash ml/retrain.sh
else
  echo "🟢 không drift → giữ model hiện tại"
fi

# 7) nạp bảng dự đoán -> Postgres (serving M9). Non-fatal: DB down không chặn pipeline.
$PY ml/load_predictions.py || echo "⚠ load_predictions lỗi -> bỏ qua (Postgres down?)"

echo "===== DAILY XONG ====="
