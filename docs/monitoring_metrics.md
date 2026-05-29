# Monitoring Metrics

This project does not implement a full monitoring stack. The operational metrics below define what should be monitored for a production-like version of the CDC lakehouse.

## PostgreSQL

| Metric | Purpose |
| --- | --- |
| Transaction rate | Baseline source write volume |
| Replication slot active state | Detect Debezium disconnects |
| WAL retained bytes by slot | Detect replication lag and disk risk |
| Source table row counts | Reconciliation baseline |
| Failed transactions | Detect source-side issues |

## Debezium and Kafka Connect

| Metric | Purpose |
| --- | --- |
| Connector state | Connector-level availability |
| Task state | Task-level failure detection |
| Source lag by LSN | CDC capture lag |
| Records produced per topic | Detect stalled CDC flow |
| Connector restart count | Stability indicator |
| Error count and dead letter count | Data contract or connector failures |

## Kafka

| Metric | Purpose |
| --- | --- |
| Topic end offset | CDC event volume |
| Consumer group lag | Spark ingestion delay |
| Broker under-replicated partitions | Broker health |
| Produce and consume rate | Throughput trend |
| Retention remaining window | Replay safety |

Expected CDC topics:

```text
cdc.public.customers
cdc.public.products
cdc.public.orders
cdc.public.order_items
cdc.public.payments
cdc.public.refunds
```

## Spark

| Metric | Purpose |
| --- | --- |
| Micro-batch duration | Processing latency |
| Input rows per second | Ingestion throughput |
| Processed rows per second | Processing capacity |
| Batch failure count | Job stability |
| Checkpoint commit latency | Object storage checkpoint health |
| Executor failures | Cluster/runtime health |

## Iceberg and Object Storage

| Metric | Purpose |
| --- | --- |
| Commit success/failure count | Table write health |
| Snapshot count per table | Metadata growth |
| Metadata file size | Metadata maintenance signal |
| Data file count and average size | Small file risk |
| Object storage request error rate | S3/MinIO health |
| Object storage latency | Commit and scan performance |

Warehouse:

```text
s3a://lakehouse/warehouse
```

Checkpoint base:

```text
s3a://lakehouse/checkpoints
```

## Data Quality

| Metric | Purpose |
| --- | --- |
| Quality check failure count | Release gate |
| Duplicate `event_id` count | CDC idempotency issue |
| Quarantine event count | Stale or out-of-order event signal |
| Source vs Silver count difference | Reconciliation issue |
| Delete event vs soft-delete count difference | Delete handling issue |

Run:

```bash
./scripts/run-quality-checks.sh
```

## Alert Candidates

- Debezium connector task is not `RUNNING`.
- Kafka consumer lag grows for more than one batch interval.
- Bronze ingestion fails.
- Silver quarantine count increases unexpectedly.
- Iceberg commit fails.
- Object storage returns repeated 5xx or timeout errors.
- Any quality check emits `FAIL`.
