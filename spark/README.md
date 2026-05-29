# Spark Runtime

STEP 6 provides common Spark Structured Streaming foundations. Later steps add Bronze, Silver, and Gold jobs on the same runtime.

## Components

- `spark/config/application.json`: local runtime defaults for Kafka, Iceberg, S3A, and checkpoint paths
- `spark/common/config.py`: config loader with environment overrides
- `spark/common/spark_session.py`: SparkSession and Iceberg Hadoop Catalog setup
- `spark/common/kafka.py`: Kafka batch and streaming source helpers
- `spark/common/cdc_parser.py`: Debezium envelope parsing helpers
- `spark/common/logging.py`: shared logging helper
- `spark/jobs/smoke_check.py`: minimal validation job
- `spark/jobs/bronze_ingestion/main.py`: Kafka CDC to Iceberg Bronze ingestion job
- `spark/jobs/silver_common/rebuild_all.py`: Bronze to Silver current/history/quarantine rebuild job
- `spark/jobs/gold_marts/rebuild.py`: Silver to Gold mart rebuild job

## Required Paths

Iceberg warehouse:

```text
s3a://lakehouse/warehouse
```

Checkpoint base:

```text
s3a://lakehouse/checkpoints
```

The default checkpoint base is S3-compatible storage. Local file checkpoints are not used by default.

## Spark Submit

Run the smoke check:

```bash
./scripts/run-spark-job.sh
```

Run another job:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/<job>.py
```

Run Bronze ingestion once for currently available Kafka records:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Check Bronze row counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/check_counts.py
```

Rebuild Silver current/history/quarantine tables from Bronze:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
```

Check Silver row counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
```

Rebuild Gold marts from Silver:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Check Gold row counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

Run data quality checks:

```bash
./scripts/run-quality-checks.sh
```

Replay downstream layers:

```bash
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
```

The script supplies Spark packages for:

- Iceberg Spark runtime
- Spark SQL Kafka source
- Hadoop AWS S3A
- AWS Java SDK bundle

The submit script sets:

```text
spark.jars.ivy=/tmp/.ivy2
```

This avoids writing Maven/Ivy cache files under the Spark container home directory.

## Configuration

Runtime values are loaded from `spark/config/application.json` and can be overridden with environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `ICEBERG_WAREHOUSE`
- `SPARK_CHECKPOINT_BASE`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`

The local default object storage endpoint is MinIO:

```text
http://minio:9000
```
