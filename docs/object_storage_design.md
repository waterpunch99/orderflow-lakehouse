# Object Storage Design

## Requirement

The Lakehouse storage layer is S3-compatible object storage, not the local filesystem.

In local development, MinIO provides the S3-compatible API. In production-like deployments, the same S3A-based configuration can be pointed to AWS S3 or another compatible object store.

## Default Local Settings

| Setting | Default |
| --- | --- |
| Runtime | MinIO |
| Bucket | `lakehouse` |
| Iceberg warehouse | `s3a://lakehouse/warehouse` |
| Checkpoint prefix | `s3a://lakehouse/checkpoints` |
| Endpoint | `S3_ENDPOINT` in `.env` |
| Access key | `S3_ACCESS_KEY` in `.env` |
| Secret key | `S3_SECRET_KEY` in `.env` |

Object storage settings are managed through `.env`, Docker Compose environment variables, and `spark/config/application.json`. They are not hardcoded inside Spark jobs.

## Spark S3A Configuration

Spark jobs include S3A settings equivalent to:

```text
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

## MinIO and AWS S3 Differences

| Topic | MinIO Local | AWS S3 |
| --- | --- | --- |
| Endpoint | Explicit local endpoint such as `http://minio:9000` | AWS regional endpoint or SDK default |
| Path-style access | Usually required for local Docker networking | Often virtual-host style by default |
| Credentials | Local development keys | IAM role, access keys, or workload identity |
| TLS | Often disabled locally | Enabled by default |
| Bucket lifecycle | Created by local init script or container | Managed by infrastructure tooling |
| Consistency and operations | Suitable for local testing | Production object storage behavior and policies apply |

## Bucket Initialization

The `lakehouse` bucket is created reproducibly by Docker Compose through the MinIO init container and can also be created by:

```bash
./scripts/create-minio-bucket.sh
```

Manual bucket creation is not required for repeatable local development.

## Iceberg Storage Layout

With Hadoop Catalog, Iceberg table metadata and data files are stored below the warehouse path. A table created under the `lakehouse` catalog will place metadata and Parquet files in the configured object storage location.

Current table directories are created by Iceberg DDL and Spark jobs under:

```text
s3a://lakehouse/warehouse/bronze/
s3a://lakehouse/warehouse/silver/
s3a://lakehouse/warehouse/gold/
```

Each Iceberg table directory contains metadata files and Parquet data files. Metadata files define valid snapshots, so object files should not be manually edited or deleted during normal operation.

## Checkpoints

Recommended checkpoint base path:

```text
s3a://lakehouse/checkpoints
```

Using S3-compatible checkpoint storage keeps the runtime close to production object storage behavior. Local file checkpoints are not used by default.

## Switching to AWS S3

When moving from MinIO to AWS S3, update:

- `S3_ENDPOINT`: use an AWS regional endpoint or omit where the runtime supports SDK defaults
- `S3_ACCESS_KEY` and `S3_SECRET_KEY`: prefer IAM roles or workload identity over static keys
- `S3_BUCKET`: use the target S3 bucket
- `ICEBERG_WAREHOUSE`: use `s3a://<bucket>/<warehouse-prefix>`
- `SPARK_CHECKPOINT_BASE`: use a separate `s3a://<bucket>/<checkpoint-prefix>`
- `spark.hadoop.fs.s3a.path.style.access`: usually `false` for AWS virtual-host style access
- `spark.hadoop.fs.s3a.connection.ssl.enabled`: `true`

Operational differences:

- AWS S3 should use bucket policies, IAM, encryption, lifecycle policies, and access logging.
- MinIO local credentials are development-only and should not be reused in production.
- S3 lifecycle rules should not delete active Iceberg metadata or checkpoint files unexpectedly.
- Bucket versioning can help recover from accidental metadata deletion, but Iceberg maintenance should still use Iceberg procedures rather than manual file operations.
