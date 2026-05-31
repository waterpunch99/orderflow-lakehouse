# 모니터링 지표

이 프로젝트는 완전한 모니터링 스택을 구현하지 않습니다. 아래 운영 지표는 CDC Lakehouse의 운영형 버전에서 모니터링해야 할 항목을 정의합니다.

## PostgreSQL

| Metric | 목적 |
| --- | --- |
| Transaction rate | Source write volume 기준선 |
| Replication slot active state | Debezium disconnect 탐지 |
| WAL retained bytes by slot | Replication lag 및 disk risk 탐지 |
| Source table row counts | Reconciliation 기준 |
| Failed transactions | Source-side issue 탐지 |

## Debezium and Kafka Connect

| Metric | 목적 |
| --- | --- |
| Connector state | Connector-level availability |
| Task state | Task-level failure detection |
| Source lag by LSN | CDC capture lag |
| Records produced per topic | 멈춘 CDC flow 탐지 |
| Connector restart count | 안정성 지표 |
| Error count and dead letter count | Data contract 또는 connector failure |

## Kafka

| Metric | 목적 |
| --- | --- |
| Topic end offset | CDC event volume |
| Consumer group lag | Spark ingestion delay |
| Broker under-replicated partitions | Broker health |
| Produce and consume rate | Throughput trend |
| Retention remaining window | Replay safety |

예상 CDC topic:

```text
cdc.public.customers
cdc.public.products
cdc.public.orders
cdc.public.order_items
cdc.public.payments
cdc.public.refunds
```

## Spark

| Metric | 목적 |
| --- | --- |
| Micro-batch duration | Processing latency |
| Input rows per second | Ingestion throughput |
| Processed rows per second | Processing capacity |
| Batch failure count | Job stability |
| Checkpoint commit latency | Object storage checkpoint health |
| Executor failures | Cluster/runtime health |

## Iceberg and Object Storage

| Metric | 목적 |
| --- | --- |
| Commit success/failure count | Table write health |
| Snapshot count per table | Metadata growth |
| Metadata file size | Metadata maintenance signal |
| Data file count and average size | Small file risk |
| Object storage request error rate | S3/MinIO health |
| Object storage latency | Commit and scan performance |

Warehouse:

```text
s3a://lakehouse/warehouse
```

Checkpoint base:

```text
s3a://lakehouse/checkpoints
```

## Data Quality

| Metric | 목적 |
| --- | --- |
| Quality check failure count | Release gate |
| Duplicate `event_id` count | CDC idempotency issue |
| Quarantine event count | Stale 또는 out-of-order event signal |
| Source vs Silver count difference | Reconciliation issue |
| Delete event vs soft-delete count difference | Delete handling issue |

실행:

```bash
./scripts/run-quality-checks.sh
```

## Alert Candidates

- Debezium connector task가 `RUNNING`이 아님
- Kafka consumer lag가 batch interval보다 오래 증가
- Bronze ingestion 실패
- Silver quarantine count가 예상치 못하게 증가
- Iceberg commit 실패
- Object storage에서 반복적인 5xx 또는 timeout 발생
- 품질 검사에서 `FAIL` 발생
