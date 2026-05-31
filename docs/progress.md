# 진행 상황

## 완료 단계

- STEP 0: 프로젝트 설계 문서와 기본 디렉터리 구조.
- STEP 1: Docker Compose 로컬 인프라.
- STEP 2: PostgreSQL source schema 및 seed data.
- STEP 3: Order, payment, refund source transaction simulator.
- STEP 4: Debezium PostgreSQL connector 설정.
- STEP 5: Kafka topic 및 CDC event contract 문서.
- STEP 6: Spark Structured Streaming 공통 기반.
- STEP 7: Bronze CDC ingestion.
- STEP 8: Silver current/history table.
- STEP 9: Gold mart.
- STEP 10: Data quality validation script.
- STEP 11: Reprocessing, failure recovery, 운영 문서.

## 남은 단계

- 없음.

## 메모

- 기본 Iceberg warehouse는 `s3a://lakehouse/warehouse`입니다.
- 로컬 S3 호환 런타임은 MinIO입니다.
- Docker Compose 인프라가 존재합니다.
- PostgreSQL source schema와 seed SQL이 존재합니다.
- PostgreSQL source transaction simulator가 존재합니다.
- Debezium PostgreSQL connector config와 registration script가 존재합니다.
- Kafka topic 문서, CDC event contract, sample topic script가 존재합니다.
- Spark common runtime foundation이 존재합니다.
- Bronze CDC ingestion job과 Iceberg DDL이 존재합니다.
- Silver current/history/quarantine rebuild job과 Iceberg DDL이 존재합니다.
- Gold mart rebuild job과 Iceberg DDL이 존재합니다.
- Data quality validation script와 rules 문서가 존재합니다.
- Reprocessing script, failure recovery 문서, monitoring metric 정의, 최종 runbook update가 존재합니다.
