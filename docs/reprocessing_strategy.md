# Reprocessing Strategy

## Principle

Bronze is the replay source. Debezium CDC events are preserved as append-only Iceberg rows, and downstream layers are deterministic derivatives of that preserved event log.

Default warehouse:

```text
s3a://lakehouse/warehouse
```

Default checkpoint base:

```text
s3a://lakehouse/checkpoints
```

Local development uses MinIO. The same paths are S3A paths, so moving to AWS S3 is a configuration change rather than a code-level storage rewrite.

## Bronze Replay

Bronze is ingested from Kafka CDC topics.

Run:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Bronze ingestion uses a checkpoint under:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

If the checkpoint exists, Spark resumes from the committed offsets. To replay old Kafka data into Bronze, the operator must intentionally reset the checkpoint and ensure Kafka still retains the needed topic data. In local development the simpler full reset path is:

```bash
./scripts/reset.sh
./scripts/start.sh
```

Then apply schema, seed data, register Debezium, generate source transactions, and run Bronze ingestion again.

## Silver Replay

Silver current/history/quarantine tables are rebuilt from Bronze. They do not read PostgreSQL directly.

Run:

```bash
./scripts/replay-silver.sh
```

The script runs:

```text
/opt/orderflow/spark/jobs/silver_common/rebuild_all.py
/opt/orderflow/spark/jobs/silver_common/check_counts.py
```

Silver rebuild behavior:

- deduplicate by `event_id`
- evaluate event order by `source_lsn`, `source_tx_id`, `event_ts`, and Kafka offset
- route stale events to `silver_quarantine_events`
- convert delete events to soft-deleted current rows
- rebuild current/history from Bronze rather than mutable PostgreSQL state

## Gold Rebuild

Gold marts are deterministic aggregates from Silver current tables.

Run:

```bash
./scripts/rebuild-gold.sh
```

The script runs:

```text
/opt/orderflow/spark/jobs/gold_marts/rebuild.py
/opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

Gold rebuild behavior:

- delete existing Gold rows
- insert fresh aggregates from Silver current tables
- exclude rows where `is_deleted = true`
- preserve Iceberg data and metadata under the S3-compatible warehouse

## Recommended Local Rebuild Sequence

After producing new PostgreSQL source transactions and ingesting Bronze:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

## Full Local Reset

Use this only when intentionally deleting all local state:

```bash
./scripts/reset.sh
./scripts/start.sh
```

The reset removes Docker named volumes, including PostgreSQL, Kafka, and MinIO data. Iceberg metadata and Parquet data in the MinIO bucket are deleted with the MinIO volume.

## AWS S3 Considerations

When moving from MinIO to AWS S3:

- point `ICEBERG_WAREHOUSE` to the target bucket prefix
- update `S3_ENDPOINT` or rely on AWS regional endpoint behavior
- prefer IAM roles or workload identity over static keys
- enable TLS
- review S3 bucket lifecycle and versioning policies
- keep checkpoints separate from table warehouse data
- avoid deleting Iceberg metadata files manually
