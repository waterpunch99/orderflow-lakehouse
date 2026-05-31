# 데이터 품질 규칙

## 목적

STEP 10은 source-to-lakehouse 품질 검사를 위한 독립 Python validation script를 추가합니다. Great Expectations는 사용하지 않습니다. 검사는 Spark로 실행되며, Iceberg 테이블은 설정된 S3 호환 warehouse에 대해 Spark SQL로 조회합니다.

기본 warehouse:

```text
s3a://lakehouse/warehouse
```

## 실행

전체 품질 검사:

```bash
./scripts/run-quality-checks.sh
```

스크립트는 다음 잡을 submit합니다.

```text
/opt/orderflow/quality/reconciliation/run_quality_checks.py
```

PostgreSQL JDBC driver를 Spark package에 추가하고 Docker Compose PostgreSQL 서비스에서 source table을 읽습니다.

## Source Checks

### orders.total_amount equals order item sum

Rule:

```text
orders.total_amount = sum(order_items.item_amount)
```

Scope: PostgreSQL source tables.

실패는 source order total과 line item 합계가 불일치한다는 뜻입니다.

### approved payment amount does not exceed order total

Rule:

```text
payments.approved_amount <= orders.total_amount
```

Scope: PostgreSQL source and Silver current tables.

실패는 승인 결제 금액이 주문 금액을 초과했다는 뜻입니다.

### cumulative refunds do not exceed approved payment amount

Rule:

```text
sum(refunds.refund_amount) <= payments.approved_amount
```

Scope: PostgreSQL source and Silver current tables.

실패는 누적 환불 금액이 승인 결제 금액을 초과했다는 뜻입니다.

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

`orders`, `payments`, `refunds`에 대해 다음을 비교합니다.

```text
PostgreSQL source row count = Silver current rows where is_deleted = false
```

Customers, products, order items는 제외합니다. STEP 8에서 Silver lifecycle table을 orders, payments, refunds에 우선 구현했기 때문입니다.

### Delete event count vs soft-deleted current rows

`orders`, `payments`, `refunds`에 대해 다음을 비교합니다.

```text
Bronze op = 'd' event count = Silver current rows where is_deleted = true
```

Downstream soft delete 정책이 정상 동작하는지 검증합니다.

## Metadata Checks

### Duplicate event_id count

각 Bronze CDC 테이블에는 중복 `event_id`가 없어야 합니다.

Scope:

- `bronze_customers_cdc`
- `bronze_products_cdc`
- `bronze_orders_cdc`
- `bronze_order_items_cdc`
- `bronze_payments_cdc`
- `bronze_refunds_cdc`

### Quarantine event count

잡은 `silver_quarantine_events` 행 수를 보고합니다.

이 metric은 정보성입니다. stale CDC 이벤트를 의도적으로 만들었다면 0이 아닐 수 있지만, 원인을 확인해야 합니다.

## Gold Checks

품질 잡은 모든 Gold mart의 row count를 보고합니다.

- `gold_daily_order_payment_summary`
- `gold_order_funnel_summary`
- `gold_payment_failure_summary`
- `gold_refund_summary`

STEP 10은 Gold row count를 가시성 검사로 다룹니다. 더 상세한 KPI reconciliation은 같은 스크립트에서 확장할 수 있습니다.

## 실패 동작

잡은 각 결과를 `PASS`, `FAIL`, `INFO`로 출력합니다.

`FAIL` 결과가 하나라도 있으면 프로세스는 nonzero status로 종료합니다. 따라서 Silver와 Gold rebuild 이후 로컬 CI 스타일 검증에 사용할 수 있습니다.
