# Lakehouse 설계

## 스토리지 요구사항

모든 Iceberg 테이블 데이터 파일과 메타데이터 파일은 S3 호환 오브젝트 스토리지에 저장해야 합니다.

로컬 기본 warehouse:

```text
s3a://lakehouse/warehouse
```

`file://` 같은 로컬 파일시스템 warehouse 경로는 프로젝트 기본값이 아니며 구현된 잡에서 사용하지 않습니다.

## Catalog

프로젝트는 Apache Iceberg Hadoop Catalog를 사용합니다.

Spark catalog 설정:

```text
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=hadoop
spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse
```

## 파일 포맷

Lakehouse 테이블은 Iceberg를 통해 Parquet 파일을 사용합니다.

## Bronze Layer

Bronze는 불변 CDC 보존 레이어입니다.

설계 원칙:

- append-only
- Debezium payload를 가능한 한 보존
- Kafka metadata 포함
- Debezium source metadata 포함
- ingestion timestamp 포함
- 결정적인 `event_id` 생성

예상 CDC 메타데이터 컬럼:

- `event_id`
- `topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `op`
- `key_json`
- `before_json`
- `after_json`
- `source_json`
- `source_table`
- `source_lsn`
- `source_tx_id`
- `event_ts`
- `ingestion_ts`
- `processing_date`

STEP 7은 source CDC topic마다 하나의 Bronze Iceberg 테이블을 구현합니다.

- `lakehouse.bronze.bronze_customers_cdc`
- `lakehouse.bronze.bronze_products_cdc`
- `lakehouse.bronze.bronze_orders_cdc`
- `lakehouse.bronze.bronze_order_items_cdc`
- `lakehouse.bronze.bronze_payments_cdc`
- `lakehouse.bronze.bronze_refunds_cdc`

테이블은 `processing_date`로 partitioning됩니다. 데이터는 Iceberg Hadoop Catalog warehouse 아래에 기록됩니다.

```text
s3a://lakehouse/warehouse
```

Bronze ingestion checkpoint:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Bronze는 Silver current-state 로직을 적용하지 않습니다. update/delete 이벤트를 포함한 CDC 이벤트를 append-only record로 보존합니다.

## Silver Layer

Silver는 CDC 이벤트를 조회 가능한 도메인 상태로 변환합니다.

예상 테이블 그룹:

- primary key별 current table
- row-level 변경 이력을 위한 history table
- 오래되었거나 유효하지 않은 이벤트를 위한 quarantine table

처리 규칙:

- `event_id` 기준 deduplication
- `source_lsn`, `source_tx_id`, `event_ts`, Kafka offset 기준 primary key별 최신 이벤트 판단
- Debezium delete operation은 soft delete로 처리
- current/history는 Bronze에서 재생성 가능해야 함
- stale event는 quarantine으로 라우팅

Current table은 원천 row의 최신 상태를 나타냅니다. History table은 변경 record를 보존해 감사와 replay 검증을 지원합니다.

STEP 8은 생명주기 엔티티용 Silver 테이블을 구현합니다.

- `lakehouse.silver.silver_orders_current`
- `lakehouse.silver.silver_orders_history`
- `lakehouse.silver.silver_payments_current`
- `lakehouse.silver.silver_payments_history`
- `lakehouse.silver.silver_refunds_current`
- `lakehouse.silver.silver_refunds_history`
- `lakehouse.silver.silver_quarantine_events`

Silver 잡은 Bronze Iceberg 테이블에서 current/history를 재생성합니다. 이 구조는 append-only Bronze 레이어에서 Silver를 재현 가능하게 만듭니다.

Current table rule:

- `event_id`로 deduplicate합니다.
- Delete 이벤트를 제외하고 `after_json`에서 source row를 파싱합니다. Delete 이벤트는 `before_json`을 사용합니다.
- `source_lsn`, `source_tx_id`, `event_ts`, Kafka offset 기준 primary key별 최신 row 하나를 유지합니다.
- Debezium delete 이벤트를 `is_deleted = true`, `deleted_at = event_ts`로 변환합니다.

History table rule:

- 유효하고 deduplicate된 모든 CDC 이벤트를 append/rebuild합니다.
- `change_op`, event metadata, `valid_from = event_ts`를 보존합니다.

Quarantine rule:

- 동일 primary key 안에서 나중에 도착한 Kafka record의 `source_lsn`이 이전 최대 `source_lsn`보다 낮으면 `silver_quarantine_events`로 라우팅합니다.
- 이 규칙은 동일 primary key의 모든 이벤트가 같은 Kafka partition에 있다는 가정에 의존합니다. Kafka offset은 partition 간 전역 ordering key가 아닙니다.
- 현재 구현은 cross-table transaction ordering을 보장하지 않습니다. 각 Silver entity의 최신 상태를 독립적으로 구성합니다.

## Gold Layer

Gold mart는 분석 준비가 된 집계를 제공합니다.

구현된 mart:

- `gold_daily_order_payment_summary`
- `gold_order_funnel_summary`
- `gold_payment_failure_summary`
- `gold_refund_summary`

예상 KPI 범위:

- 일별 주문 수
- 일별 성공 결제 주문 수
- 일별 성공 결제 금액
- 일별 결제 실패 수
- 결제 성공률
- 주문 생성에서 결제 완료까지의 전환율
- 환불 수
- 환불 금액
- 환불률

STEP 9는 Silver current table에서 결정적으로 재생성되는 Gold 잡을 구현합니다. Gold는 soft-deleted row를 제외하고 설정된 S3 호환 warehouse에 Iceberg 데이터와 메타데이터를 기록합니다.

## 재처리

Bronze는 replay source입니다. Silver와 Gold는 가변적인 source database 상태에 의존하지 않고 Bronze에서 결정적인 잡으로 재생성되어야 합니다.

Checkpoint 경로는 warehouse 데이터와 분리하고, 가능하면 다음 prefix 아래에 둡니다.

```text
s3a://lakehouse/checkpoints
```
