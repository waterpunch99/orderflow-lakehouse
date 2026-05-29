#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BASE_SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262}"
export SPARK_PACKAGES="${BASE_SPARK_PACKAGES},org.postgresql:postgresql:42.7.3"

./scripts/run-spark-job.sh /opt/orderflow/quality/reconciliation/run_quality_checks.py
