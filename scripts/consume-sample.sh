#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TOPIC="${1:-cdc.public.orders}"
MAX_MESSAGES="${2:-5}"
TIMEOUT_MS="${KAFKA_CONSUMER_TIMEOUT_MS:-10000}"

docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages "$MAX_MESSAGES" \
  --timeout-ms "$TIMEOUT_MS" \
  --property print.key=true \
  --property key.separator=' | ' \
  --property print.timestamp=true \
  --property print.partition=true \
  --property print.offset=true
