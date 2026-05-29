#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
