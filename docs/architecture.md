# 아키텍처

## 목적

이 프로젝트는 주문, 결제, 환불 데이터를 대상으로 CDC 기반 Lakehouse 파이프라인을 구현합니다. PostgreSQL의 행 단위 변경을 캡처하고, Bronze, Silver, Gold 레이어를 거쳐 분석용 Apache Iceberg 테이블을 생성합니다.

## 논리 흐름

```text
Source transactions
  -> PostgreSQL WAL
  -> Debezium PostgreSQL Connector
  -> Kafka topics with JSON CDC payloads
  -> Spark Structured Streaming
  -> Iceberg tables through Hadoop Catalog
  -> S3-compatible object storage
```

## 구성 요소

| 영역 | 기술 | 역할 |
| --- | --- | --- |
| Source DB | PostgreSQL | 주문, 결제, 환불 트랜잭션 테이블 보관 |
| CDC | Debezium PostgreSQL Connector | PostgreSQL WAL을 읽어 CDC 이벤트 발행 |
| Broker | Kafka | 원천 테이블별 CDC 이벤트 스트림 저장 |
| Processing | Spark Structured Streaming | CDC topic을 읽어 Lakehouse 테이블 작성 |
| Table Format | Apache Iceberg | 테이블 메타데이터, 스냅샷, 스키마 진화, SQL 쓰기 제공 |
| Catalog | Iceberg Hadoop Catalog | warehouse 경로 아래 Iceberg 메타데이터 저장 |
| Object Storage | MinIO local, AWS S3 later | Iceberg metadata 및 Parquet data file 저장 |

## 로컬 런타임

Docker Compose는 다음 서비스를 실행합니다.

- PostgreSQL
- Kafka
- Debezium plugin이 포함된 Kafka Connect
- Spark
- MinIO
- MinIO bucket initialization
- 선택 사항인 Kafka UI

## 스토리지 아키텍처

Lakehouse 저장소의 대상은 S3 호환 오브젝트 스토리지입니다. 로컬 개발에서는 MinIO가 S3 API를 제공합니다. Spark와 Iceberg는 Hadoop S3A로 저장소에 접근합니다.

필수 warehouse 경로:

```text
s3a://lakehouse/warehouse
```

권장 체크포인트 prefix:

```text
s3a://lakehouse/checkpoints
```

개발 안정성을 위해 이후 Spark 잡에서 로컬 볼륨 체크포인트를 사용한다면, 해당 이유를 관련 runbook에 남겨야 합니다. 로컬 체크포인트를 쓰더라도 Iceberg 테이블 데이터와 메타데이터는 S3 호환 오브젝트 스토리지에 있어야 한다는 요구사항은 변하지 않습니다.

## 레이어별 데이터 흐름

### Bronze

Bronze는 Debezium CDC 이벤트를 append-only 레코드로 저장합니다. key, before, after, source metadata, Kafka metadata, operation type 등 원본 이벤트 형태를 가능한 한 보존합니다.

### Silver

Silver 테이블은 Bronze에서 파생되며 다음 그룹으로 나뉩니다.

- current tables: primary key별 최신 상태
- history tables: 변경 이력
- quarantine tables: 오래되었거나 유효하지 않은 이벤트

Silver는 deduplication, soft delete 처리, freshness 검사, 도메인 중심 파싱을 적용합니다.

### Gold

Gold 마트는 Silver에서 파생되며 다음 분석을 지원합니다.

- 일별 주문 및 결제 요약
- 주문 funnel conversion
- 결제 실패 요약
- 환불 요약

## 재생성 원칙

Bronze는 불변 replay source입니다. Silver와 Gold는 Bronze의 CDC 이벤트를 재처리하고 결정적인 merge rule을 적용해 재생성할 수 있어야 합니다.
