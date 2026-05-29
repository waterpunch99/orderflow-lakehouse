#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

set -a
source .env
set +a

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECT_URL="${CONNECT_URL/http:\/\/kafka-connect:/http:\/\/localhost:}"
CONNECTOR_FILE="${1:-debezium/postgres-connector.json}"

python3 - "$CONNECTOR_FILE" <<'PY' | curl -fsS -X PUT -H "Content-Type: application/json" --data @- "$CONNECT_URL/connectors/orderflow-postgres-connector/config"
import json
import os
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as file:
    payload = json.load(file)

config = payload["config"]
config["database.user"] = os.environ.get("POSTGRES_USER", config["database.user"])
config["database.password"] = os.environ.get("POSTGRES_PASSWORD", config["database.password"])
config["database.dbname"] = os.environ.get("POSTGRES_DB", config["database.dbname"])

print(json.dumps(config))
PY

printf '\n'
