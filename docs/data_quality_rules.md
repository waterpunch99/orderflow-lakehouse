# Data Quality Rules

## Purpose

STEP 10 adds a self-contained Python validation script for source-to-lakehouse quality checks. Great Expectations is not used. The job runs with Spark so Iceberg tables are queried through Spark SQL against the configured S3-compatible warehouse.

Default warehouse:

```text
s3a://lakehouse/warehouse
```

## Execution

Run all quality checks:

```bash
./scripts/run-quality-checks.sh
```

The script submits:

```text
/opt/orderflow/quality/reconciliation/run_quality_checks.py
```

It adds the PostgreSQL JDBC driver to Spark packages and reads source tables from the Docker Compose PostgreSQL service.

## Source Checks

### orders.total_amount equals order item sum

Rule:

```text
orders.total_amount = sum(order_items.item_amount)
```

Scope: PostgreSQL source tables.

Failure means source order totals are inconsistent with their line items.

### approved payment amount does not exceed order total

Rule:

```text
payments.approved_amount <= orders.total_amount
```

Scope: PostgreSQL source and Silver current tables.

Failure means a payment approved more money than the order total.

### cumulative refunds do not exceed approved payment amount

Rule:

```text
sum(refunds.refund_amount) <= payments.approved_amount
```

Scope: PostgreSQL source and Silver current tables.

Failure means refund state exceeds the paid amount.

## Silver Lifecycle Checks

### PAID orders have CAPTURED payment

Rule:

```text
PAID order -> at least one CAPTURED payment
```

Scope: `silver_orders_current`, `silver_payments_current`.

### REFUNDED orders have COMPLETED refund

Rule:

```text
REFUNDED order -> at least one COMPLETED refund
```

Scope: `silver_orders_current`, `silver_refunds_current`.

## Reconciliation Checks

### Source row count vs Silver current row count

For `orders`, `payments`, and `refunds`, the job compares:

```text
PostgreSQL source row count = Silver current rows where is_deleted = false
```

Customers, products, and order items are not included because STEP 8 implemented Silver lifecycle tables only for orders, payments, and refunds.

### Delete event count vs soft-deleted current rows

For `orders`, `payments`, and `refunds`, the job compares:

```text
Bronze op = 'd' event count = Silver current rows where is_deleted = true
```

This validates the downstream soft delete policy.

## Metadata Checks

### Duplicate event_id count

Each Bronze CDC table must not contain duplicate `event_id` values.

Scope:

- `bronze_customers_cdc`
- `bronze_products_cdc`
- `bronze_orders_cdc`
- `bronze_order_items_cdc`
- `bronze_payments_cdc`
- `bronze_refunds_cdc`

### Quarantine event count

The job reports the number of rows in `silver_quarantine_events`.

This metric is informational. A nonzero count may be valid when stale CDC events were intentionally produced, but it should be investigated.

## Gold Checks

The quality job reports row counts for all Gold marts:

- `gold_daily_order_payment_summary`
- `gold_order_funnel_summary`
- `gold_payment_failure_summary`
- `gold_refund_summary`

STEP 10 treats Gold row counts as visibility checks. More detailed KPI reconciliation can be extended from the same script if needed.

## Failure Behavior

The job prints each result with `PASS`, `FAIL`, or `INFO`.

If any `FAIL` result exists, the process exits with a nonzero status. This makes it suitable for local CI-style execution after Silver and Gold rebuilds.
