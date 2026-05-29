from __future__ import annotations

from spark.common.config import load_config
from spark.common.cdc_parser import parse_debezium_json, with_event_id, with_kafka_metadata
from spark.common.kafka import read_kafka_batch
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session


def main() -> None:
    logger = get_logger("smoke_check")
    config = load_config()
    spark = build_spark_session(config, "smoke-check")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("warehouse=%s", config.warehouse)
    logger.info("checkpoint_base=%s", config.checkpoint_base)
    logger.info("kafka_bootstrap_servers=%s", config.kafka.bootstrap_servers)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog_name}.system_check")
    namespaces = spark.sql(f"SHOW NAMESPACES IN {config.catalog_name}").collect()
    logger.info("iceberg_namespaces=%s", [row[0] for row in namespaces])

    kafka_df = read_kafka_batch(spark, config)
    logger.info("kafka_batch_schema=%s", kafka_df.schema.simpleString())
    logger.info("kafka_batch_count=%s", kafka_df.count())

    parsed_df = with_event_id(with_kafka_metadata(parse_debezium_json(kafka_df)))
    parsed_df.select("event_id", "topic", "op", "source_table", "source_lsn").limit(5).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
