#!/usr/bin/env bash
# Launch the dashboards + a Cloudflare quick tunnel (public *.trycloudflare.com).
set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8501}"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared not found. Installing (macOS/brew)..."
  brew install cloudflared
fi

# Streamlit in background
../.venv/bin/streamlit run Home.py --server.port "$PORT" --server.headless true &
ST_PID=$!
trap 'kill $ST_PID 2>/dev/null || true' EXIT

# Wait for the port, then open the tunnel (its output prints the public URL)
for _ in $(seq 1 30); do
  if curl -sf "http://localhost:$PORT" >/dev/null 2>&1; then break; fi
  sleep 1
done

echo "Streamlit up on :$PORT — opening Cloudflare quick tunnel..."
cloudflared tunnel --url "http://localhost:$PORT"
