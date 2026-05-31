# Runbook

이 runbook은 STEP 0부터 STEP 11까지의 로컬 런타임, replay flow, validation flow, recovery entry point를 다룹니다.

## End-to-End Verification

샘플 환경 파일 생성:

```bash
cp .env.example .env
```

서비스 시작:

```bash
./scripts/start.sh
```

Debezium connector 등록:

```bash
./scripts/register-connector.sh
```

전체 검증 flow 실행:

```bash
./scripts/verify-all.sh demo-001
```

Wrapper는 connector 상태 확인, source simulator, Bronze ingestion, Silver replay, Gold rebuild, quality check를 순서대로 실행합니다.

예상 최종 quality summary:

```text
quality_check_summary total=24 failed=0
```

동일한 수동 실행 순서:

```bash
./scripts/check-connector.sh
./scripts/run-simulator.sh --run-id demo-001
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

## STEP 1 Local Infrastructure

### Start Services

```bash
./scripts/start.sh
```

시작되는 서비스:

- PostgreSQL
- Kafka
- Debezium plugin이 포함된 Kafka Connect
- Spark master and worker
- MinIO
- MinIO bucket initialization
- Kafka UI

### Stop Services

```bash
./scripts/stop.sh
```

### Reset Local Runtime

```bash
./scripts/reset.sh
```

Container와 named volume을 제거합니다. PostgreSQL data, Kafka data, MinIO object data가 삭제됩니다.

### Create MinIO Bucket Again

```bash
./scripts/create-minio-bucket.sh
```

Bucket init process는 `.env`에서 bucket 값을 읽습니다.

```text
S3_BUCKET=lakehouse
```

예상 Iceberg warehouse:

```text
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
```

## Service Checks

### PostgreSQL

```bash
docker compose exec postgres psql -U orderflow -d orderflow -c "select version();"
```

Host access port:

```text
POSTGRES_PORT=15432
```

Logical replication 확인:

```bash
docker compose exec postgres psql -U orderflow -d orderflow -c "show wal_level;"
```

예상 값:

```text
logical
```

### Kafka

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Kafka Connect

```bash
curl -fsS http://localhost:8083/connectors
```

Connector 등록은 STEP 4에서 수행합니다.

### MinIO

Console:

```text
http://localhost:9001
```

Credential은 `.env`에서 읽습니다.

```text
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

Bucket check:

```bash
docker compose run --rm minio-init
```

### Spark

Spark master UI:

```text
http://localhost:8080
```

Spark worker UI:

```text
http://localhost:8081
```

Spark container로 전달되는 S3/Iceberg 값:

```text
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=${S3_ACCESS_KEY}
S3_SECRET_KEY=${S3_SECRET_KEY}
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
SPARK_CHECKPOINT_BASE=s3a://lakehouse/checkpoints
```

### Kafka UI

```text
http://localhost:8088
```

## MinIO vs AWS S3

로컬 런타임은 path-style access와 Docker-network endpoint를 사용하는 MinIO 기반입니다. AWS S3로 이동할 때는 `.env`와 Spark S3A 설정에서 endpoint, credential 또는 IAM role, TLS, bucket name을 조정합니다. 자세한 내용은 `docs/object_storage_design.md`를 참고합니다.

## STEP 3 Source Transaction Simulator

Simulator 실행:

```bash
./scripts/run-simulator.sh
```

고정 run id로 실행:

```bash
./scripts/run-simulator.sh --run-id demo-001
```

Simulator는 PostgreSQL에만 write하며 Kafka message를 직접 생성하지 않습니다.

실행 후 주문 상태 확인:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select order_status, count(*)
from orders
group by order_status
order by order_status;"
```

결제 상태 확인:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select payment_status, count(*)
from payments
group by payment_status
order by payment_status;"
```

환불 상태 확인:

```bash
docker compose exec -T postgres psql -U orderflow -d orderflow -c "
select refund_status, count(*)
from refunds
group by refund_status
order by refund_status;"
```

## STEP 4 Debezium Connector

PostgreSQL connector 등록 또는 업데이트:

```bash
./scripts/register-connector.sh
```

Connector 상태 확인:

```bash
./scripts/check-connector.sh
```

Connector는 `orderflow_publication`의 여섯 source table을 읽고 `cdc` prefix의 table CDC event를 발행합니다.

예상 table topic:

```text
cdc.public.customers
cdc.public.products
cdc.public.orders
cdc.public.order_items
cdc.public.payments
cdc.public.refunds
```

Connector는 tombstone record를 비활성화합니다.

```text
tombstones.on.delete=false
```

Delete 이벤트는 여전히 Debezium `op = d` record로 발행됩니다. Downstream Silver는 이를 soft delete로 변환합니다.

## STEP 5 Kafka Topics and Samples

Topic 목록:

```bash
./scripts/list-topics.sh
```

CDC topic sample 소비:

```bash
./scripts/consume-sample.sh cdc.public.orders 5
```

새 source change 생성 후 확인:

```bash
./scripts/run-simulator.sh --run-id sample-001
./scripts/consume-sample.sh cdc.public.payments 5
```

CDC event contract는 `docs/cdc_event_contract.md`, topic 이름과 metadata rule은 `docs/kafka_topics.md`에 문서화되어 있습니다.

## STEP 6 Spark Common Runtime

Spark smoke check:

```bash
./scripts/run-spark-job.sh
```

검증 항목:

- Spark가 Docker Compose를 통해 시작되는지
- Iceberg Hadoop Catalog가 `s3a://lakehouse/warehouse`를 가리키는지
- S3A 설정이 MinIO를 가리키는지
- Kafka source package를 사용할 수 있는지
- Kafka CDC topic을 batch로 읽을 수 있는지

기본 checkpoint base:

```text
s3a://lakehouse/checkpoints
```

Spark package dependency는 `scripts/run-spark-job.sh`가 주입합니다. Script는 Spark container 안에서 `/tmp/.ivy2`를 Ivy cache path로 사용해 container home directory write 권한에 의존하지 않게 합니다.

## STEP 7 Bronze CDC Ingestion

현재 Kafka CDC record를 Bronze로 1회 적재:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Job은 다음 DDL로 Bronze namespace와 table을 생성합니다.

```text
iceberg/ddl/bronze_tables.sql
```

Bronze Iceberg tables:

```text
lakehouse.bronze.bronze_customers_cdc
lakehouse.bronze.bronze_products_cdc
lakehouse.bronze.bronze_orders_cdc
lakehouse.bronze.bronze_order_items_cdc
lakehouse.bronze.bronze_payments_cdc
lakehouse.bronze.bronze_refunds_cdc
```

Checkpoint path:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

Bronze row count 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/check_counts.py
```

Check job은 Bronze table별 row count, distinct `event_id` count, null `event_id` count를 보고합니다.

## STEP 8 Silver Current, History, and Quarantine

Bronze에서 모든 Silver lifecycle table 재생성:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
```

Entity별 재생성:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_orders/main.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_payments/main.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_refunds/main.py
```

Silver count 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
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

Silver job은 rebuild job입니다. Bronze에서 current/history row를 삭제 후 재생성하므로 layer를 재현 가능하게 유지합니다.

Incremental orders current upsert 예제:

```bash
./scripts/upsert-silver-orders.sh --once
```

이 예제는 Iceberg `MERGE INTO` 기반 운영형 upsert pattern을 보여주며, 전체 검증 경로의 deterministic rebuild를 대체하지 않습니다.

## STEP 9 Gold Marts

Silver current table에서 Gold mart 재생성:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Gold count 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

Gold tables:

```text
lakehouse.gold.gold_daily_order_payment_summary
lakehouse.gold.gold_order_funnel_summary
lakehouse.gold.gold_payment_failure_summary
lakehouse.gold.gold_refund_summary
```

Gold job은 기존 row를 삭제하고 Silver current table에서 결정적 aggregate를 insert하는 rebuild job입니다.

## STEP 10 Data Quality Checks

Source, Silver, Gold validation check 실행:

```bash
./scripts/run-quality-checks.sh
```

Quality job은 Spark JDBC로 PostgreSQL source table을 읽고, Spark SQL로 Iceberg table을 읽습니다. PostgreSQL JDBC package는 wrapper script가 추가합니다.

Source CDC data가 바뀐 뒤 quality check 전에는 Silver와 Gold를 재생성합니다.

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
./scripts/run-quality-checks.sh
```

Rule은 `docs/data_quality_rules.md`에 문서화되어 있습니다.

## STEP 11 Reprocessing and Recovery

Bronze에서 Silver replay:

```bash
./scripts/replay-silver.sh
```

Silver에서 Gold rebuild:

```bash
./scripts/rebuild-gold.sh
```

새 CDC data ingestion 이후 권장 검증 순서:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```

운영 문서:

- `docs/reprocessing_strategy.md`
- `docs/failure_recovery.md`
- `docs/monitoring_metrics.md`
- `docs/object_storage_design.md`

Local full reset:

```bash
./scripts/reset.sh
./scripts/start.sh
```

이 명령은 PostgreSQL, Kafka, MinIO object data를 포함한 Docker named volume을 삭제합니다. 로컬 포트폴리오 환경을 의도적으로 초기화할 때만 사용합니다.
