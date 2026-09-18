#!/usr/bin/env bash
# retrain — Piece 4. Luồng A tự động: pool -> stats -> clean -> features -> train -> promote-if-better.
# Gọi khi cờ drift bật. KHÔNG đè model cũ mù — promote.py gate theo RMSE.
set -euo pipefail
cd "$(dirname "$0")/.."          # về project root
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
PY="${PY:-.venv/bin/python}"

# Roots (khớp ml/config.py). Prod override qua .env; default = local cũ.
LAKE="${LAKE_ROOT:-data/lake}"
META="${META_DIR:-models}"

echo "=== RETRAIN $(date +%F) ==="

# 1) fit stats làm sạch trên TOÀN BỘ pool (median/mode/IQR mới) -> JSON điều khiển (META)
$PY ml/impute_fit.py --src "$LAKE/listings_pool" --out "$META/impute_stats.json"

# 2) áp stats -> listings_clean/sale (train mode: lọc IQR + điền khuyết)
$PY ml/impute_apply.py --src "$LAKE/listings_pool" --out "$LAKE/listings_clean/sale" --mode train

# 3) feature engineering + as-of join macro -> listings_features/sale (path từ config)
$PY ml/feature_pipeline.py

# 4) train + tuning -> MODEL_STORE/v<today>/ + metrics.json (META)
$PY ml/train.py

# 5) gate: promote nếu RMSE mới <= prod, else giữ cũ
$PY ml/promote.py --new-version "$(date +%F)"

echo "=== RETRAIN XONG ==="
