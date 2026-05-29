# Runbook

This runbook covers the local runtime, replay flow, validation flow, and recovery entry points for the complete STEP 0 through STEP 11 project.

## STEP 1 Local Infrastructure

## End-to-End Verification

Create `.env` from the sample file if it does not exist:

```bash
cp .env.example .env
```

Start services:

```bash
./scripts/start.sh
```

Register the Debezium connector:

```bash
./scripts/register-connector.sh
```

Run the full verification flow:

```bash
./scripts/verify-all.sh demo-001
```

The wrapper runs connector status check, source simulator, Bronze ingestion, Silver replay, Gold rebuild, and quality checks.

Expected final quality summary:

```text
quality_check_summary total=24 failed=0
```

Equivalent manual sequence:

```bash
./scripts/check-connector.sh
./scripts/run-simulator.sh --run-id demo-001
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

### Start Services

```bash
./scripts/start.sh
```

This starts:

- PostgreSQL
- Kafka
- Kafka Connect with Debezium plugin
- Spark master and worker
- MinIO
- MinIO bucket initialization
- Kafka UI

### Stop Services

```bash
./scripts/stop.sh
```

### Reset Local Runtime

```bash
./scripts/reset.sh
```

This removes containers and named volumes. PostgreSQL data, Kafka data, and MinIO object data will be deleted.

### Create MinIO Bucket Again

```bash
./scripts/create-minio-bucket.sh
```

The bucket init process creates the bucket from `.env`:

```text
S3_BUCKET=lakehouse
```

The expected Iceberg warehouse is:

```text
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
```

## Service Checks

### PostgreSQL

```bash
docker compose exec postgres psql -U orderflow -d orderflow -c "select version();"
```

Host access uses the port configured in `.env`:

```text
POSTGRES_PORT=15432
```

Logical replication settings are enabled at container startup for later Debezium use:

```bash
docker compose exec postgres psql -U orderflow -d orderflow -c "show wal_level;"
```

Expected value:

```text
logical
```

### Kafka

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Kafka Connect

```bash
curl -fsS http://localhost:8083/connectors
```

At STEP 1 this should return an empty connector list. Connector registration is handled in STEP 4.

### MinIO

Console:

```text
http://localhost:9001
```

Credentials are loaded from `.env`:

```text
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

Bucket check:

```bash
docker compose run --rm minio-init
```

### Spark

Spark master UI:

```text
http://localhost:8080
```

Spark worker UI:

```text
http://localhost:8081
```

S3 and Iceberg values are passed to Spark containers as environment variables:

```text
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=${S3_ACCESS_KEY}
S3_SECRET_KEY=${S3_SECRET_KEY}
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
SPARK_CHECKPOINT_BASE=s3a://lakehouse/checkpoints
```

Spark job-level package dependencies and SparkSession configuration are implemented in STEP 6.

### Kafka UI

```text
http://localhost:8088
```

## MinIO vs AWS S3

The local runtime uses MinIO with path-style access and a Docker-network endpoint. When moving to AWS S3, update `.env` and Spark S3A settings for the AWS endpoint, credentials or IAM role, TLS, and bucket name. See `docs/object_storage_design.md`.

## STEP 3 Source Transaction Simulator

The simulator uses only the Python standard library and the running Docker Compose PostgreSQL service.

Run the simulator:

```bash
./scripts/run-simulator.sh
```

Run with a stable run id:

```bash
./scripts/run-simulator.sh --run-id demo-001
```

The simulator writes only to PostgreSQL. It does not produce Kafka messages directly.

Basic verification after a run:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select order_status, count(*)
from orders
group by order_status
order by order_status;"
```

Check payment state changes:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select payment_status, count(*)
from payments
group by payment_status
order by payment_status;"
```

Check generated refund records:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select refund_status, count(*)
from refunds
group by refund_status
order by refund_status;"
```

## STEP 4 Debezium Connector

Register or update the PostgreSQL connector:

```bash
./scripts/register-connector.sh
```

Check connector status:

```bash
./scripts/check-connector.sh
```

The connector reads the six source tables from `orderflow_publication` and emits table CDC events with topic prefix `cdc`.

Expected table topics after snapshot or new source transactions:

```text
cdc.public.customers
cdc.public.products
cdc.public.orders
cdc.public.order_items
cdc.public.payments
cdc.public.refunds
```

The connector disables tombstone records:

```text
tombstones.on.delete=false
```

Delete events are still emitted as Debezium `op = d` records. Downstream Silver processing converts them to soft deletes.

## STEP 5 Kafka Topics and Samples

List Kafka topics:

```bash
./scripts/list-topics.sh
```

Consume sample messages from a CDC topic:

```bash
./scripts/consume-sample.sh cdc.public.orders 5
```

Generate a fresh source change and consume it:

```bash
./scripts/run-simulator.sh --run-id sample-001
./scripts/consume-sample.sh cdc.public.payments 5
```

The CDC event contract is documented in `docs/cdc_event_contract.md`. Topic names and metadata rules are documented in `docs/kafka_topics.md`.

## STEP 6 Spark Common Runtime

Run the Spark smoke check:

```bash
./scripts/run-spark-job.sh
```

The smoke check validates:

- Spark can start through Docker Compose.
- Iceberg Hadoop Catalog points to `s3a://lakehouse/warehouse`.
- S3A settings point to MinIO.
- Kafka source packages are available.
- Kafka CDC topics can be read as a batch.

The default checkpoint base is:

```text
s3a://lakehouse/checkpoints
```

Spark package dependencies are supplied by `scripts/run-spark-job.sh`.

The script uses `/tmp/.ivy2` as the Ivy cache path inside the Spark container so package resolution does not depend on the container home directory being writable.

## STEP 7 Bronze CDC Ingestion

Run Bronze ingestion once for available Kafka CDC records:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

The job creates the Bronze namespace and tables from:

```text
iceberg/ddl/bronze_tables.sql
```

Bronze Iceberg tables:

```text
lakehouse.bronze.bronze_customers_cdc
lakehouse.bronze.bronze_products_cdc
lakehouse.bronze.bronze_orders_cdc
lakehouse.bronze.bronze_order_items_cdc
lakehouse.bronze.bronze_payments_cdc
lakehouse.bronze.bronze_refunds_cdc
```

The checkpoint path is:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Check Bronze row counts with Spark SQL:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/check_counts.py
```

The check reports row counts, distinct `event_id` counts, and null `event_id` counts per Bronze table. STEP 8 will add Silver-specific verification.

## STEP 8 Silver Current, History, and Quarantine

Rebuild all Silver lifecycle tables from Bronze:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
```

Rebuild one entity:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_orders/main.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_payments/main.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_refunds/main.py
```

Check Silver counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
```

Silver tables:

```text
lakehouse.silver.silver_orders_current
lakehouse.silver.silver_orders_history
lakehouse.silver.silver_payments_current
lakehouse.silver.silver_payments_history
lakehouse.silver.silver_refunds_current
lakehouse.silver.silver_refunds_history
lakehouse.silver.silver_quarantine_events
```

The Silver jobs are rebuild jobs. They delete and recreate current/history rows from Bronze so the layer stays reproducible.

## STEP 9 Gold Marts

Rebuild Gold marts from Silver current tables:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Check Gold counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

Gold tables:

```text
lakehouse.gold.gold_daily_order_payment_summary
lakehouse.gold.gold_order_funnel_summary
lakehouse.gold.gold_payment_failure_summary
lakehouse.gold.gold_refund_summary
```

The Gold jobs are rebuild jobs. They delete existing Gold rows and insert deterministic aggregates from Silver current tables.

## STEP 10 Data Quality Checks

Run all source, Silver, and Gold validation checks:

```bash
./scripts/run-quality-checks.sh
```

The quality job reads PostgreSQL source tables through Spark JDBC and reads Iceberg tables through Spark SQL. The PostgreSQL JDBC package is added by the wrapper script.

Before running quality checks, rebuild Silver and Gold if source CDC data changed:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
./scripts/run-quality-checks.sh
```

Rules are documented in `docs/data_quality_rules.md`.

## STEP 11 Reprocessing and Recovery

Replay Silver from Bronze:

```bash
./scripts/replay-silver.sh
```

Rebuild Gold from Silver:

```bash
./scripts/rebuild-gold.sh
```

Recommended validation sequence after new CDC data is ingested:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

Operational documents:

- `docs/reprocessing_strategy.md`
- `docs/failure_recovery.md`
- `docs/monitoring_metrics.md`
- `docs/object_storage_design.md`

Local full reset:

```bash
./scripts/reset.sh
./scripts/start.sh
```

This deletes Docker named volumes, including PostgreSQL, Kafka, and MinIO object data. Use it only when intentionally resetting the local portfolio environment.
