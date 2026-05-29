#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
