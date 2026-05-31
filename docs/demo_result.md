# Demo Result

이 문서는 로컬 end-to-end demo 실행에서 기대하는 증거를 기록합니다. 포트폴리오 리뷰와 면접 설명에 사용할 수 있도록 구성했습니다.

## 환경

Runtime:

- Docker Compose
- PostgreSQL 16
- Debezium PostgreSQL Connector 2.7
- Kafka 7.6.1
- Spark 3.5.3
- Apache Iceberg with Hadoop Catalog
- MinIO S3-compatible object storage

Storage:

```text
bucket=lakehouse
warehouse=s3a://lakehouse/warehouse
checkpoint_base=s3a://lakehouse/checkpoints
```

## Demo Command

인프라 시작 및 connector 등록:

```bash
cp .env.example .env
./scripts/start.sh
./scripts/register-connector.sh
```

End-to-end demo 실행:

```bash
./scripts/verify-all.sh demo-001
```

스크립트 실행 흐름:

```text
check connector
run source transaction simulator
ingest Kafka CDC into Bronze Iceberg tables
rebuild Silver current/history/quarantine tables
rebuild Gold mart tables
run data quality checks
```

## CDC Topics

예상 테이블 CDC topic:

```text
cdc.public.customers
cdc.public.products
cdc.public.orders
cdc.public.order_items
cdc.public.payments
cdc.public.refunds
```

확인 명령:

```bash
./scripts/list-topics.sh
./scripts/consume-sample.sh cdc.public.orders 5
```

## Lakehouse Tables

Bronze tables:

```text
lakehouse.bronze.bronze_customers_cdc
lakehouse.bronze.bronze_products_cdc
lakehouse.bronze.bronze_orders_cdc
lakehouse.bronze.bronze_order_items_cdc
lakehouse.bronze.bronze_payments_cdc
lakehouse.bronze.bronze_refunds_cdc
```

Silver tables:

```text
lakehouse.silver.silver_orders_current
lakehouse.silver.silver_orders_history
lakehouse.silver.silver_payments_current
lakehouse.silver.silver_payments_history
lakehouse.silver.silver_refunds_current
lakehouse.silver.silver_refunds_history
lakehouse.silver.silver_quarantine_events
```

Gold tables:

```text
lakehouse.gold.gold_daily_order_payment_summary
lakehouse.gold.gold_order_funnel_summary
lakehouse.gold.gold_payment_failure_summary
lakehouse.gold.gold_refund_summary
```

## 대표 Row Count

검증된 로컬 실행 기준:

```text
silver_orders_current=9
silver_payments_current=9
silver_refunds_current=3
silver_quarantine_events=0
gold_daily_order_payment_summary=3
gold_order_funnel_summary=3
gold_payment_failure_summary=1
gold_refund_summary=1
```

확인 명령:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/check_counts.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

## 데이터 품질 결과

예상 quality summary:

```text
quality_check_summary total=24 failed=0
```

품질 검증 항목:

- source order total과 order item 합계 비교
- payment approved amount와 order total 비교
- 누적 refund amount와 payment approved amount 비교
- paid order가 captured payment를 가지는지 확인
- refunded order가 completed refund를 가지는지 확인
- source와 Silver active row count reconciliation
- Bronze 중복 `event_id` 검사
- quarantine count 보고
- Gold mart row count 보고

직접 실행:

```bash
./scripts/run-quality-checks.sh
```

## Object Storage Evidence

Iceberg data와 metadata는 S3A를 통해 MinIO에 저장됩니다.

```text
s3a://lakehouse/warehouse
```

Checkpoint data는 별도 경로에 저장됩니다.

```text
s3a://lakehouse/checkpoints
```

이 분리는 replay를 지원하고 streaming checkpoint state와 Iceberg table metadata가 섞이는 문제를 방지합니다.

## Interview Notes

설명할 핵심 포인트:

- Bronze는 append-only이며 replay를 위해 Debezium CDC payload를 보존합니다.
- Silver current table은 primary key별 최신 상태를 재구성합니다.
- Delete CDC 이벤트는 물리 삭제가 아니라 soft delete로 변환됩니다.
- Kafka offset은 동일 primary key와 partition 안에서만 tie-breaker로 사용합니다.
- Silver와 Gold는 로컬 재현성을 위해 deterministic rebuild 방식입니다.
- `silver_orders_current`에는 운영형 upsert 패턴을 보여주는 incremental Iceberg `MERGE INTO` 예제가 있습니다.
