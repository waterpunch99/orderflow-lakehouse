# CDC Order Payment Lakehouse

PostgreSQL source database changes are captured by Debezium, delivered through Kafka, and processed by Spark into Apache Iceberg tables on S3-compatible object storage.

This project is a portfolio-grade local data engineering project for an order, payment, and refund CDC lakehouse. The default runtime is Docker Compose with MinIO as local S3-compatible storage. The design keeps storage settings configurable so the same architecture can later move to AWS S3.

## What This Demonstrates

- End-to-end CDC pipeline: PostgreSQL -> Debezium -> Kafka -> Spark -> Iceberg
- Lakehouse layering: Bronze raw CDC, Silver current/history/quarantine, Gold marts
- S3-compatible storage by default: MinIO with `s3a://lakehouse/warehouse`
- CDC handling: deduplication, soft delete, stale event quarantine, replayable downstream tables
- Operational readiness: replay scripts, quality checks, recovery docs, monitoring metric definitions

## Quick Start

Prerequisites:

- Docker and Docker Compose
- Bash-compatible shell

Create local environment config:

```bash
cp .env.example .env
```

Start local infrastructure:

```bash
./scripts/start.sh
```

Run an end-to-end CDC flow:

```bash
./scripts/register-connector.sh
./scripts/run-simulator.sh --run-id demo-001
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

Or run the verification wrapper after services are up and the connector is registered:

```bash
./scripts/verify-all.sh demo-001
```

Expected quality summary:

```text
quality_check_summary total=24 failed=0
```

Representative table counts from the verified local run:

```text
silver_orders_current=9
silver_payments_current=9
silver_refunds_current=3
silver_quarantine_events=0
gold_daily_order_payment_summary=3
gold_order_funnel_summary=3
gold_payment_failure_summary=1
gold_refund_summary=1
```

## Architecture

```text
PostgreSQL
  -> Debezium PostgreSQL Connector
  -> Kafka CDC topics
  -> Spark Structured Streaming
  -> Apache Iceberg Hadoop Catalog
  -> S3-compatible object storage
```

Default local object storage:

- Runtime: MinIO
- Bucket: `lakehouse`
- Iceberg warehouse: `s3a://lakehouse/warehouse`
- Recommended checkpoint prefix: `s3a://lakehouse/checkpoints`
- File format: Parquet
- CDC payload format: JSON

Iceberg table data files and metadata files must be stored in S3-compatible object storage. Local `file://` warehouse paths are not used as the default.

## Domain Scope

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `refunds`

## Lakehouse Layers

- Bronze: append-only Debezium CDC event preservation.
- Silver: current and history tables derived from Bronze.
- Gold: analytics marts for order funnel, payment failure, and refund summary.

## Core Processing Rules

- Delete CDC events are handled as soft deletes.
- Duplicate events are removed by `event_id`.
- Event freshness is evaluated with `source_lsn`, `source_tx_id`, `event_ts`, and Kafka offset.
- Current tables store the latest state by primary key.
- History tables preserve row-level changes.
- Events older than the current row are treated as stale and routed to quarantine.
- Silver and Gold are designed to be rebuildable from Bronze.

## Project Layout

```text
.
|-- debezium/          # Debezium connector configs
|-- docs/              # Architecture, design, operation, and recovery documents
|-- iceberg/ddl/       # Iceberg table DDL
|-- postgres/          # Source schema and seed SQL
|-- quality/           # Data quality checks
|-- scripts/           # Local operation and replay scripts
|-- simulator/         # Source transaction simulator
`-- spark/             # Spark streaming/rebuild jobs and shared utilities
```

## Local Runtime

The project runs locally with Docker Compose.

Start services:

```bash
./scripts/start.sh
```

Stop services:

```bash
./scripts/stop.sh
```

Reset local volumes:

```bash
./scripts/reset.sh
```

See [docs/runbook.md](docs/runbook.md) for service checks.

Register Debezium connector after PostgreSQL schema is applied:

```bash
./scripts/register-connector.sh
./scripts/check-connector.sh
```

Typical end-to-end local flow after services are running:

```bash
./scripts/register-connector.sh
./scripts/run-simulator.sh --run-id demo-001
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

Verification wrapper:

```bash
./scripts/verify-all.sh demo-001
```

## Operations

- Reprocessing strategy: [docs/reprocessing_strategy.md](docs/reprocessing_strategy.md)
- Failure recovery: [docs/failure_recovery.md](docs/failure_recovery.md)
- Monitoring metrics: [docs/monitoring_metrics.md](docs/monitoring_metrics.md)
- Object storage design: [docs/object_storage_design.md](docs/object_storage_design.md)

## Interview Discussion Points

- Why Bronze keeps Debezium payloads append-only.
- How `event_id`, `source_lsn`, `source_tx_id`, `event_ts`, and Kafka offset are used.
- Why delete events become soft deletes in Silver.
- How stale events are routed to quarantine.
- How Silver and Gold can be rebuilt from Bronze.
- What changes when moving from MinIO to AWS S3.
- How Iceberg metadata and Parquet data files are stored in object storage.

## Current Status

STEP 11 completes the portfolio scope: PostgreSQL source schema, seed data, source transaction simulator, Debezium connector registration, Kafka topic inspection, Spark common runtime, Bronze CDC ingestion, Silver current/history rebuilds, Gold mart rebuilds, data quality validation scripts, and operation/recovery documentation.

See [docs/progress.md](docs/progress.md) for step progress.
