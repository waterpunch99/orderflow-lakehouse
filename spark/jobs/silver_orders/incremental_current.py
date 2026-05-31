from __future__ import annotations

import argparse

from pyspark.sql import DataFrame

from spark.common.config import AppConfig, load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.silver_common.processor import build_current, deduplicate_events, parse_entity_events
from spark.jobs.silver_common.specs import ORDERS_SPEC
from spark.jobs.silver_common.sql import execute_sql_file


TARGET_COLUMNS = [
    *ORDERS_SPEC.data_columns,
    "is_deleted",
    "deleted_at",
    "event_id",
    "source_lsn",
    "source_tx_id",
    "event_ts",
    "kafka_offset",
    "ingestion_ts",
    "silver_updated_at",
]

NEWER_THAN_TARGET_CONDITION = """
    COALESCE(s.source_lsn, CAST(-1 AS BIGINT)) > COALESCE(t.source_lsn, CAST(-1 AS BIGINT))
    OR (
        COALESCE(s.source_lsn, CAST(-1 AS BIGINT)) = COALESCE(t.source_lsn, CAST(-1 AS BIGINT))
        AND COALESCE(s.source_tx_id, CAST(-1 AS BIGINT)) > COALESCE(t.source_tx_id, CAST(-1 AS BIGINT))
    )
    OR (
        COALESCE(s.source_lsn, CAST(-1 AS BIGINT)) = COALESCE(t.source_lsn, CAST(-1 AS BIGINT))
        AND COALESCE(s.source_tx_id, CAST(-1 AS BIGINT)) = COALESCE(t.source_tx_id, CAST(-1 AS BIGINT))
        AND COALESCE(s.event_ts, TIMESTAMP '1970-01-01 00:00:00')
            > COALESCE(t.event_ts, TIMESTAMP '1970-01-01 00:00:00')
    )
    OR (
        COALESCE(s.source_lsn, CAST(-1 AS BIGINT)) = COALESCE(t.source_lsn, CAST(-1 AS BIGINT))
        AND COALESCE(s.source_tx_id, CAST(-1 AS BIGINT)) = COALESCE(t.source_tx_id, CAST(-1 AS BIGINT))
        AND COALESCE(s.event_ts, TIMESTAMP '1970-01-01 00:00:00')
            = COALESCE(t.event_ts, TIMESTAMP '1970-01-01 00:00:00')
        AND COALESCE(s.kafka_offset, CAST(-1 AS BIGINT)) > COALESCE(t.kafka_offset, CAST(-1 AS BIGINT))
    )
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally upsert orders CDC events from Bronze into silver_orders_current."
    )
    parser.add_argument("--once", action="store_true", help="Process available Bronze rows once and then stop.")
    parser.add_argument(
        "--ddl",
        default="/opt/orderflow/iceberg/ddl/silver_tables.sql",
        help="Path to Silver Iceberg DDL file inside the Spark container.",
    )
    parser.add_argument(
        "--checkpoint-name",
        default="silver_orders_current_incremental",
        help="Checkpoint path suffix below SPARK_CHECKPOINT_BASE.",
    )
    return parser.parse_args()


def _source_table(config: AppConfig) -> str:
    return f"{config.catalog_name}.bronze.{ORDERS_SPEC.bronze_table}"


def _target_table(config: AppConfig) -> str:
    return f"{config.catalog_name}.silver.{ORDERS_SPEC.current_table}"


def latest_orders_current(batch_df: DataFrame) -> DataFrame:
    events_df = deduplicate_events(parse_entity_events(batch_df, ORDERS_SPEC))
    return build_current(events_df, ORDERS_SPEC)


def merge_batch(batch_df: DataFrame, batch_id: int, config: AppConfig) -> None:
    logger = get_logger("silver-orders-incremental")
    if batch_df.isEmpty():
        logger.info("batch_id=%s empty batch", batch_id)
        return

    updates_df = latest_orders_current(batch_df).persist()
    try:
        update_count = updates_df.count()
        if update_count == 0:
            logger.info("batch_id=%s no valid order events", batch_id)
            return

        view_name = f"silver_orders_current_updates_{batch_id}"
        updates_df.createOrReplaceTempView(view_name)

        assignments = ",\n            ".join(f"t.{column} = s.{column}" for column in TARGET_COLUMNS)
        insert_columns = ", ".join(TARGET_COLUMNS)
        insert_values = ", ".join(f"s.{column}" for column in TARGET_COLUMNS)

        batch_df.sparkSession.sql(
            f"""
            MERGE INTO {_target_table(config)} t
            USING {view_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED AND ({NEWER_THAN_TARGET_CONDITION})
              THEN UPDATE SET
                {assignments}
            WHEN NOT MATCHED
              THEN INSERT ({insert_columns})
              VALUES ({insert_values})
            """
        )
        logger.info("batch_id=%s merged rows=%s target=%s", batch_id, update_count, _target_table(config))
    finally:
        updates_df.unpersist()


def main() -> None:
    args = parse_args()
    logger = get_logger("silver-orders-incremental")
    config = load_config()
    spark = build_spark_session(config, "silver-orders-incremental")
    spark.sparkContext.setLogLevel("WARN")

    execute_sql_file(spark, args.ddl)
    logger.info("source=%s", _source_table(config))
    logger.info("target=%s", _target_table(config))
    logger.info("checkpoint=%s", config.checkpoint_path(args.checkpoint_name))

    bronze_orders = spark.readStream.table(_source_table(config))
    writer = (
        bronze_orders.writeStream.foreachBatch(lambda df, batch_id: merge_batch(df, batch_id, config))
        .option("checkpointLocation", config.checkpoint_path(args.checkpoint_name))
        .queryName("silver_orders_current_incremental")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
