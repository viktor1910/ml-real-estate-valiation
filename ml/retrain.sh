#!/usr/bin/env bash
# retrain — Piece 4. Luồng A tự động: pool -> stats -> clean -> features -> train -> promote-if-better.
# Gọi khi cờ drift bật. KHÔNG đè model cũ mù — promote.py gate theo RMSE.
set -euo pipefail
cd "$(dirname "$0")/.."          # về project root
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
PY="${PY:-.venv/bin/python}"

echo "=== RETRAIN $(date +%F) ==="

# 1) fit stats làm sạch trên TOÀN BỘ pool (median/mode/IQR mới)
$PY ml/impute_fit.py --src data/lake/listings_pool --out models/impute_stats.json

# 2) áp stats -> listings_clean/sale (train mode: lọc IQR + điền khuyết)
$PY ml/impute_apply.py --src data/lake/listings_pool --out data/lake/listings_clean/sale --mode train

# 3) feature engineering + as-of join macro -> listings_features/sale
$PY ml/feature_pipeline.py

# 4) train + tuning -> models/v<today>/ + metrics.json
$PY ml/train.py

# 5) gate: promote nếu RMSE mới <= prod, else giữ cũ
$PY ml/promote.py --new-version "$(date +%F)"

echo "=== RETRAIN XONG ==="
