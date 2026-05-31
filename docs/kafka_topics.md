# Kafka Topic

이 문서는 Debezium이 생성하는 Kafka topic과 Spark가 CDC 레코드를 읽을 때 보존해야 하는 메타데이터를 정의합니다.

## Bootstrap Servers

Docker Compose 내부:

```text
kafka:9092
```

호스트에서 접근:

```text
localhost:29092
```

프로젝트 스크립트는 Kafka 컨테이너 안에서 실행되므로 내부 bootstrap server를 사용합니다.

## Topic Naming

Debezium topic prefix:

```text
cdc
```

테이블 topic 패턴:

```text
<topic.prefix>.<schema>.<table>
```

## Source Table Topics

| Source table | Kafka topic | Primary key |
| --- | --- | --- |
| `public.customers` | `cdc.public.customers` | `customer_id` |
| `public.products` | `cdc.public.products` | `product_id` |
| `public.orders` | `cdc.public.orders` | `order_id` |
| `public.order_items` | `cdc.public.order_items` | `order_item_id` |
| `public.payments` | `cdc.public.payments` | `payment_id` |
| `public.refunds` | `cdc.public.refunds` | `refund_id` |

Transaction metadata가 활성화되어 다음 topic이 생성될 수 있습니다.

```text
cdc.transaction
```

테이블 topic은 Bronze ingestion의 주요 입력입니다. Transaction metadata는 필요 시 별도로 보존하거나 이후 join할 수 있습니다.

## Topic 생성

로컬 Compose 런타임은 Kafka auto topic creation을 활성화합니다. Debezium은 connector가 source table snapshot을 수행하거나 WAL 변경을 수신할 때 테이블 topic을 생성합니다.

Topic 목록 확인:

```bash
./scripts/list-topics.sh
```

특정 topic 상세 확인:

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic cdc.public.orders
```

## 샘플 소비

Topic에서 소량의 메시지 소비:

```bash
./scripts/consume-sample.sh cdc.public.orders 5
```

스크립트는 Kafka console consumer의 metadata 출력 옵션을 사용해 key/value JSON과 메타데이터를 함께 출력합니다.

## Kafka Record Metadata

Spark는 Bronze에서 다음 Kafka record field를 보존해야 합니다.

- `topic`
- `partition`
- `offset`
- `timestamp`
- `timestampType`
- `key`
- `value`

Bronze 컬럼명은 다음 규칙을 사용합니다.

- `topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `key_json`
- `before_json`, `after_json`, `source_json` 등 파싱된 value field

## Ordering Notes

Kafka ordering은 topic partition 내부에서만 보장됩니다. Silver latest-state 로직은 Kafka offset만으로 순서를 판단하면 안 됩니다.

Freshness 및 conflict 처리는 다음 값을 고려합니다.

- Debezium source LSN
- Debezium source transaction id
- Debezium event timestamp
- Kafka topic, partition, offset

## Partitioning Assumptions

로컬 Docker Compose 런타임은 단순성을 위해 Kafka auto topic creation을 사용합니다. 이 포트폴리오 로컬 환경에서는 운영자가 broker default를 변경하거나 topic을 다중 partition으로 미리 만들지 않는 한 테이블 topic은 single-partition처럼 동작한다고 가정합니다.

운영형 multi-partition topic에서는 다음 계약이 필요합니다.

- 동일 source table primary key의 CDC record는 동일 Kafka partition에 기록되어야 합니다.
- Kafka offset ordering은 해당 partition 내부에서만 유효합니다.
- Silver ordering 판단은 전체 topic/table이 아니라 단일 primary key 범위에서 수행합니다.
- `orders` 이벤트와 `payments` 이벤트처럼 서로 다른 entity 간 순서는 Kafka offset으로 추론하지 않습니다.
- 테이블 topic을 여러 partition으로 미리 생성한다면 Debezium record key를 기준으로 partitioning해 동일 primary key가 한 partition에 남아야 합니다.

Silver stale-event 로직은 동일 primary key 내 도착 순서를 비교하므로 이 가정이 중요합니다. 해당 primary key의 이벤트가 같은 partition에 있을 때만 Kafka offset을 마지막 tie-breaker로 안전하게 사용할 수 있습니다.

## 현재 구현 범위

구현된 Silver 잡은 동일 primary key row에 대해 다음 ordering tuple을 사용합니다.

```text
source_lsn, source_tx_id, event_ts, kafka_offset
```

이 tuple은 current-state 재구성을 위한 것이며 전역 이벤트 타임라인을 만들기 위한 것이 아닙니다. Transaction metadata는 진단 및 향후 확장을 위해 Bronze에 보존하지만, 현재 Silver 잡은 테이블 간 transaction-level ordering을 재구성하지 않습니다.

## Tombstones

Connector 설정:

```text
tombstones.on.delete=false
```

이 CDC topic에서는 Kafka tombstone record를 기대하지 않습니다. Delete 이벤트는 여전히 `op = d`인 일반 Debezium record로 나타납니다.
