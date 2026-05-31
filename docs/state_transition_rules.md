# 상태 전이 규칙

상태 전이 검사는 simulator, Silver 처리, 데이터 품질 검사에서 사용하는 도메인 검증 규칙입니다. PostgreSQL constraint를 대체하지는 않지만 기대하는 비즈니스 흐름을 정의합니다.

## Order Status

권장 상태 값:

- `CREATED`
- `PENDING_PAYMENT`
- `PAID`
- `CANCELLED`
- `REFUND_REQUESTED`
- `REFUNDED`

기대 전이:

```text
CREATED -> PENDING_PAYMENT
PENDING_PAYMENT -> PAID
PENDING_PAYMENT -> CANCELLED
PAID -> REFUND_REQUESTED
REFUND_REQUESTED -> REFUNDED
```

잘못되었거나 의심스러운 전이는 검증 스크립트에서 탐지하거나 이후 단계의 품질 리포트로 라우팅해야 합니다.

## Payment Status

권장 상태 값:

- `REQUESTED`
- `APPROVED`
- `CAPTURED`
- `FAILED`
- `CANCELLED`

기대 전이:

```text
REQUESTED -> APPROVED
APPROVED -> CAPTURED
REQUESTED -> FAILED
APPROVED -> CANCELLED
```

`PAID` 주문은 최소 하나의 `CAPTURED` 결제를 가져야 합니다.

## Refund Status

권장 상태 값:

- `REQUESTED`
- `APPROVED`
- `COMPLETED`
- `REJECTED`

기대 전이:

```text
REQUESTED -> APPROVED
APPROVED -> COMPLETED
REQUESTED -> REJECTED
```

`REFUNDED` 주문은 최소 하나의 `COMPLETED` 환불을 가져야 합니다.

## Delete 처리

Delete CDC 이벤트는 Silver나 Gold에서 물리 삭제로 적용하지 않습니다. 대신 다음과 같은 soft delete 컬럼으로 표현합니다.

- `is_deleted`
- `deleted_at`
- `delete_event_id`

이 방식은 감사 가능성을 유지하고 하위 테이블을 재생성 가능하게 만듭니다.

## 중복 및 오래된 이벤트 처리

이벤트는 `event_id`로 deduplicate합니다.

Freshness 비교는 다음 값을 고려해야 합니다.

- `source_lsn`
- `source_tx_id`
- `event_ts`
- Kafka `offset`

동일 primary key의 현재 row보다 오래된 이벤트가 들어오면 current state를 덮어쓰지 않고 quarantine에 기록합니다.

이 비교는 하나의 primary key 범위에 한정됩니다. Kafka offset은 동일 primary key와 동일 Kafka partition으로 범위가 제한된 뒤 마지막 tie-breaker로만 사용합니다. 서로 다른 partition의 offset은 전역 순서로 비교할 수 없습니다.
