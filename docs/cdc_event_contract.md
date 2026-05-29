# CDC Event Contract

This document defines the CDC event contract for Debezium PostgreSQL events consumed from Kafka. STEP 4 configures the connector. STEP 5 defines topic structure, required fields, event identity, and operation handling rules.

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

The initial snapshot captures existing source rows. Later insert, update, and delete transactions are read from PostgreSQL WAL.

## Included Tables

- `public.customers`
- `public.products`
- `public.orders`
- `public.order_items`
- `public.payments`
- `public.refunds`

## Expected Topics

With topic prefix `cdc`, Debezium emits table events to:

- `cdc.public.customers`
- `cdc.public.products`
- `cdc.public.orders`
- `cdc.public.order_items`
- `cdc.public.payments`
- `cdc.public.refunds`

Transaction metadata is enabled and may create a transaction metadata topic such as:

- `cdc.transaction`

## Kafka Record Contract

Spark must read these Kafka fields:

| Kafka field | Bronze field | Purpose |
| --- | --- | --- |
| `topic` | `topic` | Source CDC topic |
| `partition` | `kafka_partition` | Kafka partition for deterministic record identity |
| `offset` | `kafka_offset` | Kafka offset for deterministic record identity and tie-break ordering |
| `timestamp` | `kafka_timestamp` | Kafka broker record timestamp |
| `key` | `key_json` | Debezium key JSON |
| `value` | parsed CDC payload | Debezium value JSON |

## Payload Format

Kafka Connect uses JSON converters with schemas disabled.

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

Exact `before` and `after` fields differ by table.

## Debezium Value Fields

| Field | Description | Bronze handling |
| --- | --- | --- |
| `before` | Row image before update/delete | Store as `before_json` |
| `after` | Row image after snapshot/insert/update | Store as `after_json` |
| `source` | Source metadata such as table, tx id, LSN, timestamp | Store as `source_json` and selected typed columns |
| `op` | Debezium operation code | Store as `op` |
| `ts_ms` | Debezium event processing timestamp in milliseconds | Convert to `event_ts` |
| `transaction` | Optional transaction metadata | Preserve when present |

Important `source` fields for PostgreSQL:

| Source field | Target meaning |
| --- | --- |
| `schema` | Source schema, expected `public` |
| `table` | Source table name |
| `txId` | Source transaction id |
| `lsn` | PostgreSQL log sequence number |
| `ts_ms` | Source change timestamp |

## Operation Codes

| op | Meaning | Lakehouse handling |
| --- | --- | --- |
| `r` | Snapshot read | Bronze append, Silver upsert candidate |
| `c` | Insert/create | Bronze append, Silver upsert candidate |
| `u` | Update | Bronze append, Silver current update and history append |
| `d` | Delete | Bronze append, Silver soft delete |

## Operation Handling

### `r`

Snapshot read events initialize downstream state. They are insert/upsert candidates in Silver current tables and should be preserved in Bronze.

### `c`

Create events represent source inserts. They are insert/upsert candidates in Silver current tables and history append candidates.

### `u`

Update events represent source row updates. Silver should compare freshness with the current row for the same primary key. Fresh updates modify current state and append history. Stale updates go to quarantine.

### `d`

Delete events represent source physical deletes. Downstream tables must not physically delete rows because this project keeps an auditable Lakehouse. Silver converts delete events to soft delete fields.

## Delete and Tombstone Policy

Connector setting:

```text
tombstones.on.delete=false
```

Debezium delete events are emitted with `op = d`. Tombstone records are disabled for this project because the downstream Lakehouse does not need Kafka log compaction tombstones in Bronze.

Silver and Gold must not physically delete rows because of delete CDC events. Delete events are converted to soft delete fields such as:

- `is_deleted`
- `deleted_at`
- `delete_event_id`

## Transaction Metadata

Connector setting:

```text
provide.transaction.metadata=true
```

Transaction metadata helps reason about source transaction boundaries. Spark processing should preserve transaction fields in Bronze when present. Silver latest-state logic still uses row-level ordering fields such as source LSN, source transaction id, event timestamp, and Kafka offset.

## Spark Required Fields

Spark should read Kafka metadata and Debezium payload fields needed for later Bronze and Silver processing:

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

Important source metadata from Debezium `source`:

- source connector name
- source database
- source schema
- source table
- source transaction id, mapped from PostgreSQL `source.txId`
- source LSN, mapped from PostgreSQL `source.lsn`
- source timestamp

## Event ID Rule

The primary Bronze event id is based on immutable Kafka record coordinates:

```text
event_id = sha256(topic || ':' || partition || ':' || offset)
```

This rule is deterministic for a Kafka record and supports Bronze deduplication.

Spark should also derive a source fingerprint for diagnostics and replay comparison:

```text
source_event_fingerprint = sha256(source_table || ':' || primary_key_json || ':' || op || ':' || source_lsn || ':' || event_ts)
```

`event_id` remains the deduplication key for Kafka-ingested Bronze records. The source fingerprint is useful when validating replays where Kafka offsets may differ.

## Table Primary Key Mapping

| Source table | Primary key field |
| --- | --- |
| `customers` | `customer_id` |
| `products` | `product_id` |
| `orders` | `order_id` |
| `order_items` | `order_item_id` |
| `payments` | `payment_id` |
| `refunds` | `refund_id` |

## Freshness Rule

Silver current tables must compare events for the same primary key using:

- `source_lsn`
- `source_tx_id`
- `event_ts`
- Kafka `offset`

Older events must not overwrite newer current rows. They are routed to quarantine as stale events.

Recommended ordering comparator for a single primary key:

```text
source_lsn asc, source_tx_id asc, event_ts asc, kafka_offset asc
```

When an incoming event has a lower freshness tuple than the current Silver row, it is treated as stale.
