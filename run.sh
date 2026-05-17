#!/usr/bin/env bash
#
# Starts the rocketmq broker + all client services with color-coded logs.
#
# Usage: ./run.sh
#

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

# Kill any stale process on broker port
PID=$(lsof -ti :8080 2>/dev/null || true)
if [ -n "$PID" ]; then
  echo "Killing existing process on :8080 (PID $PID)..."
  kill "$PID" 2>/dev/null || true
  sleep 1
fi

npx -y concurrently \
  --names "broker,order,payment,notify" \
  --prefix-colors "magenta,cyan,yellow,green" \
  --prefix "[{name}]" \
  --kill-others \
  "cargo run --manifest-path ${ROOT}/Cargo.toml" \
  "sleep 2 && deno run --allow-net ${ROOT}/client/order_service.ts" \
  "sleep 2 && deno run --allow-net ${ROOT}/client/payment_service.ts" \
  "sleep 2 && deno run --allow-net ${ROOT}/client/notification_service.ts"
