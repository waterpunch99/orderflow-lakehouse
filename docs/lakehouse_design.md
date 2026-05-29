# Lakehouse Design

## Storage Requirement

All Iceberg table data files and metadata files must be stored in S3-compatible object storage.

Default local warehouse:

```text
s3a://lakehouse/warehouse
```

Local filesystem warehouse paths such as `file://` are not the project default and should not be used for implemented jobs.

## Catalog

The project uses Apache Iceberg Hadoop Catalog.

Expected Spark catalog configuration in later steps:

```text
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=hadoop
spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse
```

## File Format

Lakehouse tables use Parquet files through Iceberg.

## Bronze Layer

Bronze is the immutable CDC preservation layer.

Design principles:

- append-only
- preserve Debezium payload as much as practical
- include Kafka metadata
- include source metadata from Debezium
- include ingestion timestamp
- generate deterministic `event_id`

Expected CDC metadata columns:

- `event_id`
- `topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `op`
- `key_json`
- `before_json`
- `after_json`
- `source_json`
- `source_table`
- `source_lsn`
- `source_tx_id`
- `event_ts`
- `ingestion_ts`
- `processing_date`

STEP 7 implements one Bronze Iceberg table per source CDC topic:

- `lakehouse.bronze.bronze_customers_cdc`
- `lakehouse.bronze.bronze_products_cdc`
- `lakehouse.bronze.bronze_orders_cdc`
- `lakehouse.bronze.bronze_order_items_cdc`
- `lakehouse.bronze.bronze_payments_cdc`
- `lakehouse.bronze.bronze_refunds_cdc`

The tables are partitioned by `processing_date`. They are written under the Iceberg Hadoop Catalog warehouse:

```text
s3a://lakehouse/warehouse
```

The Bronze ingestion checkpoint is under:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Bronze does not apply Silver current-state logic. It preserves CDC events as append-only records, including update and delete events.

## Silver Layer

Silver converts CDC events into queryable domain state.

Expected table groups:

- current tables by primary key
- history tables for row-level changes
- quarantine table for stale or invalid events

Processing rules:

- deduplicate by `event_id`
- evaluate latest event by `source_lsn`, `source_tx_id`, `event_ts`, and Kafka offset
- use soft delete for Debezium delete operations
- keep current and history rebuildable from Bronze
- route stale events to quarantine

Current tables represent the latest known source row state. History tables preserve change records and support auditing and replay validation.

STEP 8 implements Silver for lifecycle entities:

- `lakehouse.silver.silver_orders_current`
- `lakehouse.silver.silver_orders_history`
- `lakehouse.silver.silver_payments_current`
- `lakehouse.silver.silver_payments_history`
- `lakehouse.silver.silver_refunds_current`
- `lakehouse.silver.silver_refunds_history`
- `lakehouse.silver.silver_quarantine_events`

The Silver jobs rebuild current/history from Bronze Iceberg tables. This keeps Silver reproducible from the append-only Bronze layer.

Current table rule:

- Deduplicate by `event_id`.
- Parse source row from `after_json`, except delete events use `before_json`.
- Keep one latest row per primary key by `source_lsn`, `source_tx_id`, `event_ts`, and Kafka offset.
- Convert Debezium delete events to `is_deleted = true` and `deleted_at = event_ts`.

History table rule:

- Append/rebuild all valid deduplicated CDC events.
- Preserve `change_op`, event metadata, and `valid_from = event_ts`.

Quarantine rule:

- Within the same primary key, if a later-arriving Kafka record has `source_lsn` lower than a previously observed max `source_lsn`, route it to `silver_quarantine_events`.

## Gold Layer

Gold marts provide analytics-ready aggregates.

Implemented marts:

- `gold_daily_order_payment_summary`
- `gold_order_funnel_summary`
- `gold_payment_failure_summary`
- `gold_refund_summary`

Expected KPI coverage:

- daily order count
- daily successful payment order count
- daily successful payment amount
- daily payment failure count
- payment success rate
- order created to payment completed conversion rate
- refund count
- refund amount
- refund rate

STEP 9 implements Gold as deterministic rebuild jobs from Silver current tables. Gold excludes soft-deleted rows and writes Iceberg data and metadata to the configured S3-compatible warehouse.

## Reprocessing

Bronze is the replay source. Silver and Gold should be recreated by deterministic jobs from Bronze without relying on mutable source database state.

Checkpoint paths should be separated from warehouse data, preferably under:

```text
s3a://lakehouse/checkpoints
```
