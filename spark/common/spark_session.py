from __future__ import annotations

from pyspark.sql import SparkSession

from spark.common.config import AppConfig


def build_spark_session(config: AppConfig, app_suffix: str | None = None) -> SparkSession:
    app_name = config.app_name if app_suffix is None else f"{config.app_name}-{app_suffix}"
    catalog = config.catalog_name

    builder = (
        SparkSession.builder.appName(app_name)
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", config.warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.fs.s3a.endpoint", config.s3.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.s3.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(config.s3.path_style_access).lower())
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.s3.ssl_enabled).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    return builder.getOrCreate()
