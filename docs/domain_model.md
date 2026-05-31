# 도메인 모델

## 엔티티

### customers

구매자 계정을 나타냅니다. 고객 행은 주문에서 참조합니다.

Primary key:

- `customer_id`

Columns:

- `customer_name`
- `email`
- `phone`
- `customer_status`
- `created_at`
- `updated_at`

### products

판매 가능한 상품을 나타냅니다. 상품 행은 주문 아이템에서 참조합니다.

Primary key:

- `product_id`

Columns:

- `sku`
- `product_name`
- `category`
- `unit_price`
- `product_status`
- `created_at`
- `updated_at`

### orders

고객 주문을 나타냅니다. 하나의 주문은 하나 이상의 주문 아이템을 가지며 결제 및 환불 레코드와 연결될 수 있습니다.

Primary key:

- `order_id`

Columns:

- `customer_id`
- `order_status`
- `currency`
- `total_amount`
- `ordered_at`
- `created_at`
- `updated_at`

### order_items

주문 하위의 라인 아이템을 나타냅니다.

Primary key:

- `order_item_id`

Columns:

- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `item_amount`

### payments

주문의 결제 생명주기를 나타냅니다. 결제 변경은 PostgreSQL row update로 기록되고 이후 CDC 이벤트로 발행됩니다.

Primary key:

- `payment_id`

Columns:

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

결제된 주문의 환불 생명주기를 나타냅니다.

Primary key:

- `refund_id`

Columns:

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

## 도메인 관계

```text
customers 1 -- N orders
orders 1 -- N order_items
products 1 -- N order_items
orders 1 -- N payments
payments 1 -- N refunds
orders 1 -- N refunds
```

PostgreSQL 원천 스키마는 `postgres/init.sql`에 정의되어 있습니다.

## Source CDC 설계

여섯 개 원천 테이블은 모두 CDC 대상이며 primary key를 가집니다. `updated_at`은 공통 trigger로 관리됩니다. `REPLICA IDENTITY FULL`을 활성화해 Debezium이 update/delete 이벤트의 이전 row 값을 캡처할 수 있게 합니다.

Debezium publication:

```text
orderflow_publication
```

## Lakehouse 모델링 전략

원천 테이블은 Debezium topic을 통해 독립적으로 캡처됩니다. Bronze는 테이블 단위 CDC 이벤트를 보관합니다. Silver는 먼저 주요 생명주기 전이를 가진 orders, payments, refunds의 current/history 상태를 구성합니다. Gold는 Silver 상태를 join해 분석용 집계를 만듭니다.
