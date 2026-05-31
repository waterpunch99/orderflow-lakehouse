# Object Storage 설계

## 요구사항

Lakehouse storage layer는 로컬 파일시스템이 아니라 S3 호환 오브젝트 스토리지입니다.

로컬 개발에서는 MinIO가 S3 호환 API를 제공합니다. 운영 유사 배포에서는 동일한 S3A 기반 설정을 AWS S3 또는 다른 호환 object store로 전환할 수 있습니다.

## 기본 로컬 설정

| Setting | Default |
| --- | --- |
| Runtime | MinIO |
| Bucket | `lakehouse` |
| Iceberg warehouse | `s3a://lakehouse/warehouse` |
| Checkpoint prefix | `s3a://lakehouse/checkpoints` |
| Endpoint | `.env`의 `S3_ENDPOINT` |
| Access key | `.env`의 `S3_ACCESS_KEY` |
| Secret key | `.env`의 `S3_SECRET_KEY` |

Object storage 설정은 `.env`, Docker Compose 환경 변수, `spark/config/application.json`로 관리합니다. Spark job 내부에 hardcode하지 않습니다.

## Spark S3A Configuration

Spark job은 다음과 동등한 S3A 설정을 포함합니다.

```text
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

## MinIO와 AWS S3 차이

| Topic | MinIO Local | AWS S3 |
| --- | --- | --- |
| Endpoint | `http://minio:9000` 같은 명시적 local endpoint | AWS regional endpoint 또는 SDK default |
| Path-style access | 로컬 Docker networking에서 보통 필요 | 보통 virtual-host style 기본값 |
| Credentials | Local development key | IAM role, access key, workload identity |
| TLS | 로컬에서는 보통 비활성화 | 기본 활성화 |
| Bucket lifecycle | Local init script/container가 생성 | Infrastructure tooling으로 관리 |
| Consistency and operations | Local testing에 적합 | Production object storage behavior and policies 적용 |

## Bucket Initialization

`lakehouse` bucket은 Docker Compose의 MinIO init container로 재현 가능하게 생성되며 다음 명령으로도 생성할 수 있습니다.

```bash
./scripts/create-minio-bucket.sh
```

반복 가능한 로컬 개발에서는 수동 bucket 생성이 필요하지 않습니다.

## Iceberg Storage Layout

Hadoop Catalog를 사용하면 Iceberg table metadata와 data file이 warehouse 경로 아래에 저장됩니다. `lakehouse` catalog 아래 생성된 table은 설정된 object storage 위치에 metadata와 Parquet file을 배치합니다.

현재 table directory는 Iceberg DDL과 Spark job에 의해 다음 경로 아래 생성됩니다.

```text
s3a://lakehouse/warehouse/bronze/
s3a://lakehouse/warehouse/silver/
s3a://lakehouse/warehouse/gold/
```

각 Iceberg table directory에는 metadata file과 Parquet data file이 있습니다. Metadata file은 유효한 snapshot을 정의하므로 정상 운영 중 object file을 수동 편집하거나 삭제하지 않습니다.

## Checkpoints

권장 checkpoint base path:

```text
s3a://lakehouse/checkpoints
```

S3 호환 checkpoint storage를 사용하면 런타임을 운영 object storage 동작에 가깝게 유지할 수 있습니다. 로컬 파일 checkpoint는 기본값으로 사용하지 않습니다.

## AWS S3로 전환

MinIO에서 AWS S3로 이동할 때 변경할 항목:

- `S3_ENDPOINT`: AWS regional endpoint를 사용하거나 runtime이 SDK default를 지원하면 생략
- `S3_ACCESS_KEY`, `S3_SECRET_KEY`: static key보다 IAM role 또는 workload identity 우선
- `S3_BUCKET`: 대상 S3 bucket
- `ICEBERG_WAREHOUSE`: `s3a://<bucket>/<warehouse-prefix>`
- `SPARK_CHECKPOINT_BASE`: 별도 `s3a://<bucket>/<checkpoint-prefix>`
- `spark.hadoop.fs.s3a.path.style.access`: AWS virtual-host style access에서는 보통 `false`
- `spark.hadoop.fs.s3a.connection.ssl.enabled`: `true`

운영 차이:

- AWS S3는 bucket policy, IAM, encryption, lifecycle policy, access logging을 사용해야 합니다.
- MinIO local credential은 개발 전용이며 운영에서 재사용하지 않습니다.
- S3 lifecycle rule이 active Iceberg metadata나 checkpoint file을 예기치 않게 삭제하지 않도록 합니다.
- Bucket versioning은 실수로 metadata를 삭제했을 때 복구에 도움이 되지만, Iceberg maintenance는 수동 file 작업이 아니라 Iceberg procedure를 사용해야 합니다.
