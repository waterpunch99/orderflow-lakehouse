# 장애 복구

## Debezium Connector 장애

증상:

- `./scripts/check-connector.sh`에서 connector 또는 task 상태가 `FAILED`로 표시됨
- Kafka CDC topic에 새 record가 들어오지 않음
- PostgreSQL replication slot lag 증가

확인:

```bash
./scripts/check-connector.sh
docker compose logs kafka-connect
docker compose exec -T postgres psql -U orderflow -d orderflow -c "select slot_name, active, restart_lsn from pg_replication_slots;"
```

복구:

1. PostgreSQL이 정상이고 `wal_level`이 `logical`인지 확인합니다.
2. Connector config가 여전히 `orderflow_publication`을 가리키는지 확인합니다.
3. 로컬 일시 장애라면 Kafka Connect를 재시작합니다.
4. Config drift가 의심되면 connector를 다시 등록합니다.

```bash
./scripts/register-connector.sh
```

로컬 CDC 상태를 의도적으로 초기화하는 경우가 아니라면 replication slot을 삭제하지 마십시오.

## Kafka Consumer Lag

증상:

- Bronze ingestion이 Kafka CDC topic보다 뒤처짐
- 새 source transaction은 Kafka에 보이지만 Bronze에는 없음

확인:

```bash
./scripts/list-topics.sh
./scripts/consume-sample.sh cdc.public.orders 5
```

이 포트폴리오 런타임에서는 Bronze를 bounded catch-up job으로 실행할 수 있습니다.

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
```

복구:

1. Kafka broker와 Spark master/worker가 실행 중인지 확인합니다.
2. Bronze ingestion을 실행합니다.
3. Silver와 Gold를 재생성합니다.
4. 품질 검사를 실행합니다.

## Spark Checkpoint 장애

Bronze checkpoint path:

```text
s3a://lakehouse/checkpoints/bronze_ingestion
```

증상:

- Spark streaming job이 checkpoint state를 읽다가 실패
- 로컬 object storage reset 이후 offset이 일관되지 않아 보임

복구 옵션:

- 권장 로컬 경로: 처음부터 replay할 때 `./scripts/reset.sh`로 전체 로컬 런타임 상태를 초기화합니다.
- 대상 checkpoint reset: 영향받은 checkpoint prefix만 object storage에서 제거한 뒤, Kafka topic retention에 필요한 데이터가 남아 있으면 Bronze를 다시 실행합니다.

Checkpoint data는 Iceberg warehouse와 분리됩니다. `s3a://lakehouse/warehouse` 안이 아니라 `s3a://lakehouse/checkpoints` 아래에 유지합니다.

## Iceberg Commit 장애

증상:

- Spark job이 `INSERT`, `DELETE`, table write 중 실패
- Spark log에 metadata commit error 발생

확인:

```bash
docker compose logs spark-master
docker compose logs spark-worker
docker compose run --rm minio-init
```

복구:

1. MinIO가 정상이고 `lakehouse` bucket이 존재하는지 확인합니다.
2. Spark S3A 설정이 MinIO 또는 대상 S3 endpoint를 가리키는지 확인합니다.
3. 실패한 rebuild job을 다시 실행합니다.
4. 중단된 로컬 실행이 partial data file을 남겼다면 Parquet file을 수동 삭제하지 말고 Iceberg metadata snapshot에 의존합니다.

전체 warehouse를 의도적으로 초기화하는 경우가 아니라면 Iceberg metadata file을 수동 편집하거나 삭제하지 마십시오.

## MinIO Bucket Reset

깨끗한 로컬 reset:

```bash
./scripts/reset.sh
./scripts/start.sh
```

이 절차는 MinIO container와 bucket init flow를 다시 만듭니다. Docker named volume이 제거되므로 local object data가 삭제됩니다.

MinIO가 실행 중일 때 bucket만 다시 만들기:

```bash
./scripts/create-minio-bucket.sh
```

## Object Storage Credentials

로컬 credential은 재현성을 위해 `.env`에 저장합니다. 운영 유사 배포에서는 다음 원칙을 적용합니다.

- 실제 credential을 commit하지 않습니다.
- IAM role, workload identity, secret manager를 우선 사용합니다.
- static key를 사용한다면 주기적으로 rotate합니다.
- bucket 권한은 필요한 prefix로 제한합니다.

## 복구 순서

복구 가능한 장애 이후에는 다음 순서를 사용합니다.

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/bronze_ingestion/main.py --once
./scripts/replay-silver.sh
./scripts/rebuild-gold.sh
./scripts/run-quality-checks.sh
```
