# Kafka Topics

STEP 5 documents the Kafka topics produced by Debezium and the metadata Spark must preserve when reading CDC records.

## Bootstrap Servers

Inside Docker Compose:

```text
kafka:9092
```

From the host:

```text
localhost:29092
```

The project scripts use the Kafka container so they can use the internal bootstrap server.

## Topic Naming

Debezium topic prefix:

```text
cdc
```

Table topic pattern:

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

Transaction metadata is enabled and may create:

```text
cdc.transaction
```

The table topics are the primary inputs for Bronze ingestion. Transaction metadata can be preserved separately or joined later if needed.

## Topic Creation

Kafka auto topic creation is enabled in the local Compose runtime. Debezium creates table topics when the connector snapshots source tables or receives WAL changes.

List topics:

```bash
./scripts/list-topics.sh
```

Describe one topic:

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic cdc.public.orders
```

## Sample Consumption

Consume a small sample from a topic:

```bash
./scripts/consume-sample.sh cdc.public.orders 5
```

The script prints key and value JSON with metadata enabled by Kafka console consumer.

## Kafka Record Metadata

Spark must preserve these Kafka record fields in Bronze:

- `topic`
- `partition`
- `offset`
- `timestamp`
- `timestampType`
- `key`
- `value`

Bronze column naming should use:

- `topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `key_json`
- parsed value fields such as `before_json`, `after_json`, and `source_json`

## Ordering Notes

Kafka ordering is guaranteed only within a topic partition. Silver latest-state logic must not rely on Kafka offset alone.

Freshness and conflict handling should consider:

- Debezium source LSN
- Debezium source transaction id
- Debezium event timestamp
- Kafka topic, partition, and offset

## Tombstones

The connector sets:

```text
tombstones.on.delete=false
```

Kafka tombstone records are not expected in these CDC topics. Delete events still appear as normal Debezium records with `op = d`.
