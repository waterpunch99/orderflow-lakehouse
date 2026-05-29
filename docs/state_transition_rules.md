# State Transition Rules

State transition checks are domain validation rules used by the simulator, Silver processing, and data quality checks. They do not replace PostgreSQL constraints, but they define the expected business flow.

## Order Status

Recommended status values:

- `CREATED`
- `PENDING_PAYMENT`
- `PAID`
- `CANCELLED`
- `REFUND_REQUESTED`
- `REFUNDED`

Expected transitions:

```text
CREATED -> PENDING_PAYMENT
PENDING_PAYMENT -> PAID
PENDING_PAYMENT -> CANCELLED
PAID -> REFUND_REQUESTED
REFUND_REQUESTED -> REFUNDED
```

Invalid or suspicious transitions should be detectable by validation scripts or routed to quality reports in later steps.

## Payment Status

Recommended status values:

- `REQUESTED`
- `APPROVED`
- `CAPTURED`
- `FAILED`
- `CANCELLED`

Expected transitions:

```text
REQUESTED -> APPROVED
APPROVED -> CAPTURED
REQUESTED -> FAILED
APPROVED -> CANCELLED
```

A `PAID` order is expected to have at least one `CAPTURED` payment.

## Refund Status

Recommended status values:

- `REQUESTED`
- `APPROVED`
- `COMPLETED`
- `REJECTED`

Expected transitions:

```text
REQUESTED -> APPROVED
APPROVED -> COMPLETED
REQUESTED -> REJECTED
```

A `REFUNDED` order is expected to have at least one `COMPLETED` refund.

## Delete Handling

Delete CDC events are not applied as physical deletes in Silver or Gold. They are represented with soft delete columns such as:

- `is_deleted`
- `deleted_at`
- `delete_event_id`

This preserves auditability and makes downstream tables rebuildable.

## Duplicate and Stale Event Handling

Events are deduplicated by `event_id`.

Freshness comparison should consider:

- `source_lsn`
- `source_tx_id`
- `event_ts`
- Kafka `offset`

If an incoming event is older than the current row for the same primary key, it is treated as stale and written to quarantine instead of overwriting current state.
