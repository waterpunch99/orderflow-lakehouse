#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

set -a
source .env
set +a

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECT_URL="${CONNECT_URL/http:\/\/kafka-connect:/http:\/\/localhost:}"
CONNECTOR_NAME="${1:-orderflow-postgres-connector}"

echo "connectors:"
curl -fsS "$CONNECT_URL/connectors"
printf '\n\n'

echo "status:"
curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
printf '\n'
