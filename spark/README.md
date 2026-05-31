# Spark 런타임

이 디렉터리는 CDC Lakehouse 파이프라인의 Spark 실행 환경과 잡 코드를 포함합니다. 공통 런타임 위에서 Bronze 수집, Silver 정제, Gold 마트 생성 잡이 동작합니다.

## 구성 요소

- `spark/config/application.json`: Kafka, Iceberg, S3A, 체크포인트 경로의 로컬 기본값
- `spark/common/config.py`: 환경 변수 오버라이드를 지원하는 설정 로더
- `spark/common/spark_session.py`: SparkSession 및 Iceberg Hadoop Catalog 설정
- `spark/common/kafka.py`: Kafka 배치/스트리밍 소스 헬퍼
- `spark/common/cdc_parser.py`: Debezium envelope 파싱 헬퍼
- `spark/common/logging.py`: 공통 로깅 헬퍼
- `spark/jobs/smoke_check.py`: 최소 실행 검증 잡
- `spark/jobs/bronze_ingestion/main.py`: Kafka CDC를 Iceberg Bronze 테이블로 적재하는 잡
- `spark/jobs/silver_common/rebuild_all.py`: Bronze에서 Silver current/history/quarantine 테이블을 재생성하는 잡
- `spark/jobs/silver_orders/incremental_current.py`: Bronze orders CDC를 `silver_orders_current`에 Iceberg `MERGE INTO`로 증분 반영하는 예제
- `spark/jobs/gold_marts/rebuild.py`: Silver에서 Gold 마트를 재생성하는 잡

## 필수 경로

Iceberg warehouse:

```text
s3a://lakehouse/warehouse
```

체크포인트 기본 경로:

```text
s3a://lakehouse/checkpoints
```

기본 체크포인트 저장소는 S3 호환 오브젝트 스토리지입니다. 로컬 파일 체크포인트는 기본값으로 사용하지 않습니다.

## Spark Submit

스모크 체크 실행:

```bash
./scripts/run-spark-job.sh
```

다른 잡 실행:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/<job>.py
```

현재 Kafka에 있는 CDC 레코드를 Bronze로 1회 적재:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

Bronze 행 수 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/check_counts.py
```

Bronze에서 Silver current/history/quarantine 테이블 재생성:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/rebuild_all.py
```

Iceberg `MERGE INTO`로 `silver_orders_current`만 증분 upsert:

```bash
./scripts/upsert-silver-orders.sh --once
```

이 잡은 운영형 upsert 패턴을 보여주기 위해 orders current 테이블로 범위를 제한합니다. 전체 포트폴리오 검증 경로는 여전히 결정적인 Silver rebuild 잡을 사용합니다.

Silver 행 수 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/silver_common/check_counts.py
```

Silver에서 Gold 마트 재생성:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Gold 행 수 확인:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

데이터 품질 검사 실행:

```bash
./scripts/run-quality-checks.sh
```

하위 레이어 재처리:

```bash
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
```

`scripts/run-spark-job.sh`는 다음 Spark 패키지를 주입합니다.

- Iceberg Spark runtime
- Spark SQL Kafka source
- Hadoop AWS S3A
- AWS Java SDK bundle

또한 다음 설정을 적용합니다.

```text
spark.jars.ivy=/tmp/.ivy2
```

이를 통해 Maven/Ivy 캐시가 Spark 컨테이너 홈 디렉터리에 기록되는 문제를 피합니다.

## 설정

런타임 값은 `spark/config/application.json`에서 읽고, 다음 환경 변수로 오버라이드할 수 있습니다.

- `KAFKA_BOOTSTRAP_SERVERS`
- `ICEBERG_WAREHOUSE`
- `SPARK_CHECKPOINT_BASE`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`

로컬 기본 오브젝트 스토리지 엔드포인트는 MinIO입니다.

```text
http://minio:9000
```
