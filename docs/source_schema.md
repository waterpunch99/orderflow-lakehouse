# Source Schema

STEP 2는 CDC capture를 위한 PostgreSQL source schema를 정의합니다. Debezium이 실제 source database의 row change를 캡처해야 하므로 schema는 의도적으로 transactional하고 normalized한 형태입니다.

## Tables

| Table | Primary Key | CDC 역할 |
| --- | --- | --- |
| `customers` | `customer_id` | Customer master data |
| `products` | `product_id` | Product master data |
| `orders` | `order_id` | Order lifecycle state |
| `order_items` | `order_item_id` | Order line items |
| `payments` | `payment_id` | Payment lifecycle state |
| `refunds` | `refund_id` | Refund lifecycle state |

모든 CDC 대상 테이블은 primary key를 가집니다.

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

## CDC 적합성

Schema는 Debezium PostgreSQL CDC에 맞게 다음 선택을 적용합니다.

- 모든 target table은 안정적인 primary key를 가집니다.
- `updated_at`은 update 시 PostgreSQL trigger로 관리합니다.
- 모든 target table에 `REPLICA IDENTITY FULL`을 활성화해 Debezium이 update/delete의 전체 `before` 값을 캡처할 수 있게 합니다.
- `orderflow_publication`은 connector 사용을 위해 여섯 개 source table을 모두 포함합니다.
- Status column은 check constraint로 lifecycle 값을 명시적으로 제한합니다.
- Amount column은 numeric check로 잘못된 source data를 조기에 잡습니다.

## Logical Replication

Docker Compose는 logical replication 설정으로 PostgreSQL을 시작합니다.

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

확인:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "show wal_level;"
```

예상 값:

```text
logical
```

## 로컬 Schema 적용

STEP 1 container가 실행 중일 때:

```bash
docker compose cp postgres/init.sql postgres:/tmp/init.sql
docker compose cp postgres/seed.sql postgres:/tmp/seed.sql
docker compose exec -T postgres psql -U orderflow -d orderflow -f /tmp/init.sql
docker compose exec -T postgres psql -U orderflow -d orderflow -f /tmp/seed.sql
```

Schema script는 여섯 개 source table을 drop 후 재생성합니다. 로컬 개발 reset에만 사용합니다.

## 기본 검증

Table 목록:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "\dt"
```

Row count 확인:

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

예상 seed count:

| Table | Count |
| --- | ---: |
| `customers` | 3 |
| `products` | 4 |
| `orders` | 3 |
| `order_items` | 4 |
| `payments` | 3 |
| `refunds` | 1 |

Publication 확인:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select pubname, schemaname, tablename
from pg_publication_tables
where pubname = 'orderflow_publication'
order by tablename;"
```
