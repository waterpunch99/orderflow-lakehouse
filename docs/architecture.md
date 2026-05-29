# Architecture

## Purpose

This project demonstrates a CDC-based lakehouse pipeline for order, payment, and refund data. The system captures row-level changes from PostgreSQL and builds analytical Iceberg tables through Bronze, Silver, and Gold layers.

## Logical Flow

```text
Source transactions
  -> PostgreSQL WAL
  -> Debezium PostgreSQL Connector
  -> Kafka topics with JSON CDC payloads
  -> Spark Structured Streaming
  -> Iceberg tables through Hadoop Catalog
  -> S3-compatible object storage
```

## Components

| Area | Technology | Responsibility |
| --- | --- | --- |
| Source DB | PostgreSQL | Owns transactional order, payment, and refund tables |
| CDC | Debezium PostgreSQL Connector | Reads PostgreSQL WAL and publishes CDC events |
| Broker | Kafka | Stores CDC event streams by source table |
| Processing | Spark Structured Streaming | Reads CDC topics and writes Lakehouse tables |
| Table Format | Apache Iceberg | Provides table metadata, snapshots, schema evolution, and SQL writes |
| Catalog | Iceberg Hadoop Catalog | Stores Iceberg table metadata under the warehouse path |
| Object Storage | MinIO locally, AWS S3 later | Stores Iceberg metadata and Parquet data files |

## Local Runtime

Docker Compose will be used in later steps to run:

- PostgreSQL
- Kafka
- Kafka Connect with Debezium plugin
- Spark
- MinIO
- MinIO bucket initialization
- Optional Kafka UI

## Storage Architecture

The Lakehouse storage target is S3-compatible object storage. In local development, MinIO provides the S3 API. Spark and Iceberg access storage through Hadoop S3A.

Required warehouse path:

```text
s3a://lakehouse/warehouse
```

Recommended checkpoint prefix:

```text
s3a://lakehouse/checkpoints
```

If a later Spark job uses local volume checkpoints for development stability, the reason must be documented in the related runbook. Local checkpoints do not change the requirement that Iceberg table data and metadata live in S3-compatible object storage.

## Data Flow by Layer

### Bronze

Bronze stores Debezium CDC events as append-only records. It preserves the original event shape as much as practical, including key, before, after, source metadata, Kafka metadata, and operation type.

### Silver

Silver tables are derived from Bronze and split into:

- current tables: latest state by primary key
- history tables: change history
- quarantine tables: stale or invalid events

Silver applies deduplication, soft delete handling, freshness checks, and domain-oriented parsing.

### Gold

Gold marts are derived from Silver and support analytics such as:

- daily order and payment summary
- order funnel conversion
- payment failure summary
- refund summary

## Rebuild Principle

Bronze is the immutable replay source. Silver and Gold should be rebuildable from Bronze by replaying CDC events and applying deterministic merge rules.
