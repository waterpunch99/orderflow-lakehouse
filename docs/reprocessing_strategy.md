# 재처리 전략

## 원칙

Bronze는 replay source입니다. Debezium CDC 이벤트는 append-only Iceberg row로 보존되며, downstream layer는 보존된 event log의 결정적 파생물입니다.

기본 warehouse:

```text
s3a://lakehouse/warehouse
```

기본 checkpoint base:

```text
s3a://lakehouse/checkpoints
```

로컬 개발은 MinIO를 사용합니다. 경로는 모두 S3A 경로이므로 AWS S3로 이동할 때는 코드 수준 storage rewrite가 아니라 설정 변경으로 처리합니다.

## Bronze Replay

Bronze는 Kafka CDC topic에서 적재합니다.

실행:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Bronze ingestion checkpoint:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Checkpoint가 존재하면 Spark는 commit된 offset부터 재개합니다. 과거 Kafka 데이터를 Bronze로 replay하려면 운영자가 checkpoint를 의도적으로 reset하고 필요한 topic data가 Kafka retention에 남아 있는지 확인해야 합니다. 로컬 개발에서 단순한 전체 reset 경로는 다음과 같습니다.

```bash
./scripts/reset.sh
./scripts/start.sh
```

이후 schema, seed data, Debezium 등록, source transaction 생성, Bronze ingestion을 다시 수행합니다.

## Silver Replay

Silver current/history/quarantine table은 Bronze에서 재생성합니다. PostgreSQL을 직접 읽지 않습니다.

실행:

```bash
./scripts/replay-silver.sh
```

스크립트 실행 잡:

```text
/opt/orderflow/spark/jobs/silver_common/rebuild_all.py
/opt/orderflow/spark/jobs/silver_common/check_counts.py
```

Silver rebuild 동작:

- `event_id`로 deduplicate
- `source_lsn`, `source_tx_id`, `event_ts`, Kafka offset으로 event order 평가
- stale event를 `silver_quarantine_events`로 라우팅
- delete 이벤트를 soft-deleted current row로 변환
- mutable PostgreSQL state가 아니라 Bronze에서 current/history 재생성

## Silver Incremental Upsert 예제

주요 로컬 검증 경로는 결정적 rebuild를 사용하지만, 프로젝트에는 `silver_orders_current`를 대상으로 한 제한적 incremental upsert 예제도 포함되어 있습니다.

현재 Bronze order event를 1회 처리:

```bash
./scripts/upsert-silver-orders.sh --once
```

계속 실행:

```bash
./scripts/upsert-silver-orders.sh
```

이 잡은 append-only `bronze_orders_cdc`를 stream으로 읽고, 각 micro-batch 안에서 `order_id`별 최신 이벤트를 선택한 뒤 Iceberg `MERGE INTO`로 `silver_orders_current`에 반영합니다. Matched row는 incoming CDC event가 `source_lsn`, `source_tx_id`, `event_ts`, Kafka offset 기준으로 더 최신일 때만 update합니다. Delete 이벤트는 rebuild job과 동일하게 `is_deleted = true`로 표현합니다.

이 잡은 deterministic replay path를 대체하지 않으면서 운영형 pattern을 보여주기 위해 하나의 current table로 범위를 제한합니다.

## Gold Rebuild

Gold mart는 Silver current table에서 만든 결정적 aggregate입니다.

실행:

```bash
./scripts/rebuild-gold.sh
```

스크립트 실행 잡:

```text
/opt/orderflow/spark/jobs/gold_marts/rebuild.py
/opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

Gold rebuild 동작:

- 기존 Gold row 삭제
- Silver current table에서 fresh aggregate insert
- `is_deleted = true`인 row 제외
- S3 호환 warehouse 아래 Iceberg data와 metadata 보존

## 권장 로컬 재생성 순서

새 PostgreSQL source transaction을 만들고 Bronze에 적재한 뒤:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

## 전체 로컬 Reset

모든 로컬 상태를 의도적으로 삭제할 때만 사용합니다.

```bash
./scripts/reset.sh
./scripts/start.sh
```

Reset은 PostgreSQL, Kafka, MinIO 데이터를 포함한 Docker named volume을 제거합니다. MinIO volume이 삭제되므로 Iceberg metadata와 Parquet data도 함께 삭제됩니다.

## AWS S3 고려사항

MinIO에서 AWS S3로 이동할 때:

- `ICEBERG_WAREHOUSE`를 대상 bucket prefix로 지정
- `S3_ENDPOINT`를 업데이트하거나 AWS regional endpoint 동작에 의존
- static key보다 IAM role 또는 workload identity 우선
- TLS 활성화
- S3 bucket lifecycle 및 versioning policy 검토
- checkpoint를 table warehouse data와 분리
- Iceberg metadata file 수동 삭제 금지
