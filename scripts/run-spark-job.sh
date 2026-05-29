#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

JOB_PATH="${1:-/opt/orderflow/spark/jobs/smoke_check.py}"
shift || true

SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262}"

docker compose exec -T \
  -e PYTHONPATH=/opt/orderflow \
  -e POSTGRES_HOST="${POSTGRES_HOST:-postgres}" \
  -e POSTGRES_INTERNAL_PORT="${POSTGRES_INTERNAL_PORT:-5432}" \
  -e POSTGRES_DB="${POSTGRES_DB:-orderflow}" \
  -e POSTGRES_USER="${POSTGRES_USER:-orderflow}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-orderflow_pw}" \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --packages "$SPARK_PACKAGES" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executorEnv.PYTHONPATH=/opt/orderflow \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse="${ICEBERG_WAREHOUSE:-s3a://lakehouse/warehouse}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT:-http://minio:9000}" \
  --conf spark.hadoop.fs.s3a.access.key="${S3_ACCESS_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.secret.key="${S3_SECRET_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  "$JOB_PATH" "$@"
