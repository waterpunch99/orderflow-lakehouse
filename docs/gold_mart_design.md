# Gold Mart Design

## Purpose

Gold marts provide small, analysis-ready aggregates built from Silver current tables. The marts are rebuildable from Silver and are stored as Apache Iceberg tables in the Hadoop Catalog warehouse at `s3a://lakehouse/warehouse`.

## Source Tables

- `lakehouse.silver.silver_orders_current`
- `lakehouse.silver.silver_payments_current`
- `lakehouse.silver.silver_refunds_current`

Gold marts exclude rows where `is_deleted = true`. This keeps delete events visible in Silver while preventing physically deleted source rows from contributing to active business KPIs.

## Tables

### gold_daily_order_payment_summary

Daily order and payment KPI table keyed by `summary_date`.

Metrics:
- `order_count`
- `paid_order_count`
- `payment_success_order_count`
- `payment_success_amount`
- `payment_failed_count`
- `payment_success_rate`
- `refund_count`
- `refund_amount`
- `refund_rate`

### gold_order_funnel_summary

Order funnel table keyed by `summary_date`.

Metrics:
- `created_order_count`
- `paid_order_count`
- `payment_completed_order_count`
- `cancelled_order_count`
- `refunded_order_count`
- `order_to_paid_rate`
- `order_to_payment_completed_rate`
- `order_to_refunded_rate`

### gold_payment_failure_summary

Payment failure table keyed by `failure_date`, `payment_method`, and `failure_code`.

Metrics:
- `failed_payment_count`
- `failed_order_count`
- `failed_requested_amount`

### gold_refund_summary

Refund table keyed by `refund_date` and `refund_status`.

Metrics:
- `refund_count`
- `completed_refund_count`
- `refund_amount`
- `completed_refund_amount`
- `affected_order_count`
- `refund_rate`

## Rebuild Strategy

The Gold job creates tables if needed, deletes existing Gold rows, and inserts the latest aggregates from Silver. This is intentionally batch-oriented because Gold marts are deterministic derivatives of Silver current tables.

Run:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Validate counts:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

## Object Storage

Gold table data and Iceberg metadata are stored under the configured Iceberg warehouse:

```text
s3a://lakehouse/warehouse/gold/
```

For local development this path maps to the MinIO `lakehouse` bucket. The same Spark/Iceberg configuration can be moved to AWS S3 by changing the S3 endpoint, credentials, and bucket policy configuration documented in `docs/object_storage_design.md`.
