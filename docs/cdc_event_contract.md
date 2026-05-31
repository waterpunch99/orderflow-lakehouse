# CDC 이벤트 계약

이 문서는 Kafka에서 소비하는 Debezium PostgreSQL CDC 이벤트의 계약을 정의합니다. Connector 설정, topic 구조, 필수 field, event identity, operation 처리 규칙을 함께 다룹니다.

## Connector

Connector name:

```text
orderflow-postgres-connector
```

Connector class:

```text
io.debezium.connector.postgresql.PostgresConnector
```

Topic prefix:

```text
cdc
```

Publication:

```text
orderflow_publication
```

Replication slot:

```text
orderflow_cdc_slot
```

Snapshot mode:

```text
initial
```

초기 snapshot은 기존 source row를 캡처합니다. 이후 insert, update, delete transaction은 PostgreSQL WAL에서 읽습니다.

## 포함 테이블

- `public.customers`
- `public.products`
- `public.orders`
- `public.order_items`
- `public.payments`
- `public.refunds`

## 예상 Topic

Topic prefix가 `cdc`일 때 Debezium은 테이블 이벤트를 다음 topic으로 발행합니다.

- `cdc.public.customers`
- `cdc.public.products`
- `cdc.public.orders`
- `cdc.public.order_items`
- `cdc.public.payments`
- `cdc.public.refunds`

Transaction metadata가 활성화되어 다음과 같은 topic이 생성될 수 있습니다.

- `cdc.transaction`

## Kafka Record 계약

Spark는 다음 Kafka field를 읽어야 합니다.

| Kafka field | Bronze field | 목적 |
| --- | --- | --- |
| `topic` | `topic` | Source CDC topic |
| `partition` | `kafka_partition` | 결정적 record identity용 Kafka partition |
| `offset` | `kafka_offset` | 결정적 record identity 및 tie-break ordering |
| `timestamp` | `kafka_timestamp` | Kafka broker record timestamp |
| `key` | `key_json` | Debezium key JSON |
| `value` | parsed CDC payload | Debezium value JSON |

## Payload 형식

Kafka Connect는 schema disabled JSON converter를 사용합니다.

Key payload:

```json
{
  "customer_id": 1
}
```

Value payload shape:

```json
{
  "before": {},
  "after": {},
  "source": {},
  "op": "c",
  "ts_ms": 1710000000000,
  "transaction": {}
}
```

정확한 `before`, `after` field는 테이블마다 다릅니다.

## Debezium Value Fields

| Field | 설명 | Bronze 처리 |
| --- | --- | --- |
| `before` | update/delete 이전 row image | `before_json`으로 저장 |
| `after` | snapshot/insert/update 이후 row image | `after_json`으로 저장 |
| `source` | table, tx id, LSN, timestamp 등 source metadata | `source_json` 및 주요 typed column으로 저장 |
| `op` | Debezium operation code | `op`로 저장 |
| `ts_ms` | Debezium event processing timestamp, millisecond | `event_ts`로 변환 |
| `transaction` | 선택적 transaction metadata | 존재하면 보존 |

PostgreSQL에서 중요한 `source` field:

| Source field | 의미 |
| --- | --- |
| `schema` | Source schema, expected `public` |
| `table` | Source table name |
| `txId` | Source transaction id |
| `lsn` | PostgreSQL log sequence number |
| `ts_ms` | Source change timestamp |

## Operation Codes

| op | 의미 | Lakehouse 처리 |
| --- | --- | --- |
| `r` | Snapshot read | Bronze append, Silver upsert candidate |
| `c` | Insert/create | Bronze append, Silver upsert candidate |
| `u` | Update | Bronze append, Silver current update 및 history append |
| `d` | Delete | Bronze append, Silver soft delete |

## Operation 처리

### `r`

Snapshot read 이벤트는 downstream state를 초기화합니다. Silver current table의 insert/upsert candidate이며 Bronze에 보존해야 합니다.

### `c`

Create 이벤트는 source insert를 의미합니다. Silver current table의 insert/upsert candidate이며 history append candidate입니다.

### `u`

Update 이벤트는 source row update를 의미합니다. Silver는 동일 primary key의 current row와 freshness를 비교합니다. 더 최신 update는 current state를 변경하고 history에 추가합니다. 오래된 update는 quarantine으로 보냅니다.

### `d`

Delete 이벤트는 source physical delete를 의미합니다. 이 프로젝트는 auditable Lakehouse를 유지하므로 downstream table에서 물리 삭제하지 않습니다. Silver는 delete 이벤트를 soft delete field로 변환합니다.

## Delete 및 Tombstone 정책

Connector setting:

```text
tombstones.on.delete=false
```

Debezium delete 이벤트는 `op = d`로 발행됩니다. 이 프로젝트의 downstream Lakehouse는 Bronze에서 Kafka log compaction tombstone을 필요로 하지 않으므로 tombstone record를 비활성화합니다.

Silver와 Gold는 delete CDC 이벤트 때문에 row를 물리 삭제하면 안 됩니다. Delete 이벤트는 다음과 같은 soft delete field로 변환합니다.

- `is_deleted`
- `deleted_at`
- `delete_event_id`

## Transaction Metadata

Connector setting:

```text
provide.transaction.metadata=true
```

Transaction metadata는 source transaction boundary를 해석하는 데 도움이 됩니다. Spark processing은 존재하는 transaction field를 Bronze에 보존해야 합니다. Silver latest-state 로직은 여전히 source LSN, source transaction id, event timestamp, Kafka offset 같은 row-level ordering field를 사용합니다.

현재 Silver 구현은 Debezium transaction metadata로 전역 cross-table event order를 만들지 않습니다. Primary key별 최신 상태를 재구성합니다. 향후 `orders`, `payments`, `refunds` 간 transaction-consistent join이 필요하면 transaction metadata topic과 per-event transaction field를 설계에 포함해야 합니다.

## Spark 필수 Fields

Spark는 Bronze와 Silver 처리를 위해 다음 Kafka metadata 및 Debezium payload field를 읽어야 합니다.

- Kafka `topic`
- Kafka `partition`
- Kafka `offset`
- Kafka `timestamp`
- Kafka key JSON
- Debezium `before`
- Debezium `after`
- Debezium `source`
- Debezium `op`
- Debezium `ts_ms`
- Debezium `transaction`

Debezium `source`에서 중요한 metadata:

- source connector name
- source database
- source schema
- source table
- PostgreSQL `source.txId`에서 매핑한 source transaction id
- PostgreSQL `source.lsn`에서 매핑한 source LSN
- source timestamp

## Event ID 규칙

Primary Bronze event id는 변경되지 않는 Kafka record coordinate를 기반으로 합니다.

```text
event_id = sha256(topic || ':' || partition || ':' || offset)
```

이 규칙은 Kafka record에 대해 결정적이며 Bronze deduplication을 지원합니다.
