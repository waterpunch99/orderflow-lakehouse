from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.cdc_parser import parse_debezium_json, with_event_id, with_kafka_metadata
from spark.common.config import AppConfig, load_config
from spark.common.kafka import read_kafka_stream
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session


BRONZE_COLUMNS = [
    "event_id",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "op",
    "key_json",
    "value_json",
    "before_json",
    "after_json",
    "source_json",
    "transaction_json",
    "source_table",
    "source_lsn",
    "source_tx_id",
    "event_ts",
    "ingestion_ts",
    "processing_date",
]

TABLE_BY_SOURCE = {
    "customers": "bronze_customers_cdc",
    "products": "bronze_products_cdc",
    "orders": "bronze_orders_cdc",
    "order_items": "bronze_order_items_cdc",
    "payments": "bronze_payments_cdc",
    "refunds": "bronze_refunds_cdc",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Debezium CDC Kafka events into Iceberg Bronze tables.")
    parser.add_argument("--once", action="store_true", help="Process available Kafka records once and then stop.")
    parser.add_argument(
        "--ddl",
        default="/opt/orderflow/iceberg/ddl/bronze_tables.sql",
        help="Path to Bronze Iceberg DDL file inside the Spark container.",
    )
    parser.add_argument(
        "--checkpoint-name",
        default="bronze_ingestion",
        help="Checkpoint path suffix below SPARK_CHECKPOINT_BASE.",
    )
    return parser.parse_args()


def execute_sql_file(spark, path: str) -> None:
    sql_text = Path(path).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    for statement in statements:
        spark.sql(statement)


def bronze_dataframe(kafka_df: DataFrame) -> DataFrame:
    parsed_df = with_event_id(with_kafka_metadata(parse_debezium_json(kafka_df)))
    return (
        parsed_df.filter(F.col("value").isNotNull())
        .filter(F.col("source_table").isin(list(TABLE_BY_SOURCE.keys())))
        .withColumn("processing_date", F.to_date(F.col("ingestion_ts")))
        .select(*BRONZE_COLUMNS)
    )


def write_batch(batch_df: DataFrame, batch_id: int, config: AppConfig) -> None:
    logger = get_logger("bronze_ingestion")
    if batch_df.isEmpty():
        logger.info("batch_id=%s empty batch", batch_id)
        return

    batch_df.persist()
    try:
        for source_table, bronze_table in TABLE_BY_SOURCE.items():
            target = f"{config.catalog_name}.bronze.{bronze_table}"
            table_df = batch_df.filter(F.col("source_table") == source_table)
            count = table_df.count()
            if count == 0:
                continue
            table_df.writeTo(target).append()
            logger.info("batch_id=%s appended rows=%s target=%s", batch_id, count, target)
    finally:
        batch_df.unpersist()


def main() -> None:
    args = parse_args()
    logger = get_logger("bronze_ingestion")
    config = load_config()
    spark = build_spark_session(config, "bronze-ingestion")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("warehouse=%s", config.warehouse)
    logger.info("checkpoint=%s", config.checkpoint_path(args.checkpoint_name))
    execute_sql_file(spark, args.ddl)

    kafka_df = read_kafka_stream(spark, config)
    bronze_df = bronze_dataframe(kafka_df)

    writer = (
        bronze_df.writeStream.foreachBatch(lambda df, batch_id: write_batch(df, batch_id, config))
        .option("checkpointLocation", config.checkpoint_path(args.checkpoint_name))
        .queryName("bronze_ingestion")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
