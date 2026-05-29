from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    starting_offsets: str
    fail_on_data_loss: str
    cdc_topics: list[str]


@dataclass(frozen=True)
class S3Config:
    endpoint: str
    access_key: str
    secret_key: str
    path_style_access: bool
    ssl_enabled: bool


@dataclass(frozen=True)
class AppConfig:
    app_name: str
    catalog_name: str
    warehouse: str
    checkpoint_base: str
    kafka: KafkaConfig
    s3: S3Config

    def checkpoint_path(self, job_name: str) -> str:
        return f"{self.checkpoint_base.rstrip('/')}/{job_name}"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y"}


def load_config(path: str | Path | None = None) -> AppConfig:
    config_path = Path(path or os.getenv("ORDERFLOW_SPARK_CONFIG", "/opt/orderflow/spark/config/application.json"))
    with config_path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    kafka_raw = raw.get("kafka", {})
    s3_raw = raw.get("s3", {})

    kafka = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_raw.get("bootstrap_servers", "kafka:9092")),
        starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", kafka_raw.get("starting_offsets", "earliest")),
        fail_on_data_loss=os.getenv("KAFKA_FAIL_ON_DATA_LOSS", kafka_raw.get("fail_on_data_loss", "false")),
        cdc_topics=list(kafka_raw.get("cdc_topics", [])),
    )
    s3 = S3Config(
        endpoint=os.getenv("S3_ENDPOINT", s3_raw.get("endpoint", "http://minio:9000")),
        access_key=os.getenv("S3_ACCESS_KEY", s3_raw.get("access_key", "minioadmin")),
        secret_key=os.getenv("S3_SECRET_KEY", s3_raw.get("secret_key", "minioadmin")),
        path_style_access=_as_bool(os.getenv("S3_PATH_STYLE_ACCESS", s3_raw.get("path_style_access", True))),
        ssl_enabled=_as_bool(os.getenv("S3_SSL_ENABLED", s3_raw.get("ssl_enabled", False))),
    )

    return AppConfig(
        app_name=os.getenv("SPARK_APP_NAME", raw.get("app_name", "orderflow-lakehouse")),
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", raw.get("catalog_name", "lakehouse")),
        warehouse=os.getenv("ICEBERG_WAREHOUSE", raw.get("warehouse", "s3a://lakehouse/warehouse")),
        checkpoint_base=os.getenv("SPARK_CHECKPOINT_BASE", raw.get("checkpoint_base", "s3a://lakehouse/checkpoints")),
        kafka=kafka,
        s3=s3,
    )
