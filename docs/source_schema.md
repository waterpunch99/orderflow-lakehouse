# Source Schema

STEP 2 defines the PostgreSQL source schema for CDC capture. The schema is intentionally transactional and normalized because Debezium should capture actual row changes from the source database.

## Tables

| Table | Primary Key | CDC Role |
| --- | --- | --- |
| `customers` | `customer_id` | Customer master data |
| `products` | `product_id` | Product master data |
| `orders` | `order_id` | Order lifecycle state |
| `order_items` | `order_item_id` | Order line items |
| `payments` | `payment_id` | Payment lifecycle state |
| `refunds` | `refund_id` | Refund lifecycle state |

Every CDC target table has a primary key.

## Key Columns

### customers

- `customer_id`
- `customer_name`
- `email`
- `phone`
- `customer_status`
- `created_at`
- `updated_at`

### products

- `product_id`
- `sku`
- `product_name`
- `category`
- `unit_price`
- `product_status`
- `created_at`
- `updated_at`

### orders

- `order_id`
- `customer_id`
- `order_status`
- `currency`
- `total_amount`
- `ordered_at`
- `created_at`
- `updated_at`

### order_items

- `order_item_id`
- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `item_amount`
- `created_at`
- `updated_at`

### payments

- `payment_id`
- `order_id`
- `payment_status`
- `payment_method`
- `requested_amount`
- `approved_amount`
- `failure_code`
- `failure_message`
- `requested_at`
- `approved_at`
- `captured_at`
- `failed_at`
- `created_at`
- `updated_at`

### refunds

- `refund_id`
- `payment_id`
- `order_id`
- `refund_status`
- `refund_amount`
- `reason`
- `requested_at`
- `approved_at`
- `completed_at`
- `rejected_at`
- `created_at`
- `updated_at`

## CDC Suitability

The schema is prepared for Debezium PostgreSQL CDC with these choices:

- All target tables have stable primary keys.
- `updated_at` is maintained by a PostgreSQL trigger on updates.
- `REPLICA IDENTITY FULL` is enabled on all target tables so Debezium can capture full `before` values for updates and deletes.
- `orderflow_publication` includes all six source tables for later connector use.
- Status columns use check constraints to keep lifecycle values explicit.
- Amount columns use numeric checks to catch invalid source data early.

## Logical Replication

Docker Compose starts PostgreSQL with logical replication settings:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

Verify:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "show wal_level;"
```

Expected:

```text
logical
```

## Apply Schema Locally

With the STEP 1 containers running:

```bash
docker compose cp postgres/init.sql postgres:/tmp/init.sql
docker compose cp postgres/seed.sql postgres:/tmp/seed.sql
docker compose exec -T postgres psql -U orderflow -d orderflow -f /tmp/init.sql
docker compose exec -T postgres psql -U orderflow -d orderflow -f /tmp/seed.sql
```

The schema script drops and recreates the six source tables. Use it only for local development reset.

## Basic Verification

List tables:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "\dt"
```

Check row counts:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select 'customers' as table_name, count(*) from customers
union all select 'products', count(*) from products
union all select 'orders', count(*) from orders
union all select 'order_items', count(*) from order_items
union all select 'payments', count(*) from payments
union all select 'refunds', count(*) from refunds
order by table_name;"
```

Expected seed counts:

| Table | Count |
| --- | ---: |
| `customers` | 3 |
| `products` | 4 |
| `orders` | 3 |
| `order_items` | 4 |
| `payments` | 3 |
| `refunds` | 1 |

Check publication:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select pubname, schemaname, tablename
from pg_publication_tables
where pubname = 'orderflow_publication'
order by tablename;"
```
