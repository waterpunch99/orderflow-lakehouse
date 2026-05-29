# Failure Recovery

## Debezium Connector Failure

Symptoms:

- `./scripts/check-connector.sh` shows connector or task state as `FAILED`
- Kafka CDC topics stop receiving new records
- PostgreSQL replication slot lag increases

Checks:

```bash
./scripts/check-connector.sh
docker compose logs kafka-connect
docker compose exec -T postgres psql -U orderflow -d orderflow -c "select slot_name, active, restart_lsn from pg_replication_slots;"
```

Recovery:

1. Check PostgreSQL is healthy and `wal_level` is `logical`.
2. Check the connector config still points to `orderflow_publication`.
3. Restart Kafka Connect if this is a transient local runtime issue.
4. Re-register the connector if config drift is suspected:

```bash
./scripts/register-connector.sh
```

Do not drop the replication slot unless you are intentionally resetting local CDC state.

## Kafka Consumer Lag

Symptoms:

- Bronze ingestion falls behind Kafka CDC topics
- new source transactions are visible in Kafka but not in Bronze

Checks:

```bash
./scripts/list-topics.sh
./scripts/consume-sample.sh cdc.public.orders 5
```

For this portfolio runtime, Bronze can be run as a bounded catch-up job:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Recovery:

1. Confirm Kafka broker and Spark master/worker are running.
2. Run Bronze ingestion.
3. Rebuild Silver and Gold.
4. Run quality checks.

## Spark Checkpoint Failure

Bronze checkpoint path:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Symptoms:

- Spark streaming job fails while reading checkpoint state
- offsets appear inconsistent after local object storage reset

Recovery options:

- Preferred local path: reset all local runtime state with `./scripts/reset.sh` when replaying from scratch.
- Targeted checkpoint reset: remove only the affected checkpoint prefix from object storage, then rerun Bronze from Kafka if topic retention still contains required data.

Checkpoint data is separate from the Iceberg warehouse. Keep it under `s3a://lakehouse/checkpoints` rather than inside `s3a://lakehouse/warehouse`.

## Iceberg Commit Failure

Symptoms:

- Spark job fails during `INSERT`, `DELETE`, or table write
- metadata commit errors appear in Spark logs

Checks:

```bash
docker compose logs spark-master
docker compose logs spark-worker
docker compose run --rm minio-init
```

Recovery:

1. Confirm MinIO is healthy and the `lakehouse` bucket exists.
2. Confirm Spark S3A settings point to MinIO or the target S3 endpoint.
3. Rerun the failed rebuild job.
4. If an interrupted local run left partial data files, rely on Iceberg metadata snapshots rather than manually deleting Parquet files.

Do not manually edit or delete Iceberg metadata files unless you are intentionally resetting the full warehouse.

## MinIO Bucket Reset

For a clean local reset:

```bash
./scripts/reset.sh
./scripts/start.sh
```

This recreates the MinIO container and bucket init flow. It deletes local object data because Docker named volumes are removed.

To recreate only the bucket when MinIO is running:

```bash
./scripts/create-minio-bucket.sh
```

## Object Storage Credentials

Local credentials are stored in `.env` for reproducibility. For production-like deployments:

- do not commit real credentials
- prefer IAM roles, workload identity, or secret managers
- rotate static keys if used
- scope bucket permissions to required prefixes

## Recovery Order

After any recoverable failure, use this sequence:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```
