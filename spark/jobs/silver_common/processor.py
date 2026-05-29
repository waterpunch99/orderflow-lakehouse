from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType

from spark.common.config import AppConfig


@dataclass(frozen=True)
class SilverEntitySpec:
    entity: str
    pk: str
    bronze_table: str
    current_table: str
    history_table: str
    schema: StructType
    data_columns: list[str]


METADATA_COLUMNS = [
    "event_id",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "op",
    "key_json",
    "before_json",
    "after_json",
    "source_json",
    "source_table",
    "source_lsn",
    "source_tx_id",
    "event_ts",
    "ingestion_ts",
]


def parse_entity_events(bronze_df: DataFrame, spec: SilverEntitySpec) -> DataFrame:
    record_json = F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json"))
    parsed = F.from_json(record_json, spec.schema)
    return (
        bronze_df.withColumn("record", parsed)
        .select(*[F.col(f"record.{col}").alias(col) for col in spec.data_columns], *METADATA_COLUMNS)
        .filter(F.col(spec.pk).isNotNull())
        .withColumn("is_deleted", F.col("op") == F.lit("d"))
        .withColumn("deleted_at", F.when(F.col("op") == "d", F.col("event_ts")))
    )


def deduplicate_events(events_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("event_id").orderBy(
        F.col("source_lsn").desc_nulls_last(),
        F.col("source_tx_id").desc_nulls_last(),
        F.col("event_ts").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    return events_df.withColumn("_dedup_rank", F.row_number().over(window)).filter("_dedup_rank = 1").drop("_dedup_rank")


def split_stale_events(events_df: DataFrame, pk: str) -> tuple[DataFrame, DataFrame]:
    arrival_window = (
        Window.partitionBy(pk)
        .orderBy(F.col("kafka_partition").asc(), F.col("kafka_offset").asc())
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    with_previous = events_df.withColumn(
        "previous_max_source_lsn",
        F.max("source_lsn").over(arrival_window),
    )
    stale_df = with_previous.filter(
        F.col("previous_max_source_lsn").isNotNull()
        & F.col("source_lsn").isNotNull()
        & (F.col("source_lsn") < F.col("previous_max_source_lsn"))
    )
    valid_df = with_previous.filter(
        F.col("previous_max_source_lsn").isNull()
        | F.col("source_lsn").isNull()
        | (F.col("source_lsn") >= F.col("previous_max_source_lsn"))
    ).drop("previous_max_source_lsn")
    return valid_df, stale_df


def build_current(valid_df: DataFrame, spec: SilverEntitySpec) -> DataFrame:
    latest_window = Window.partitionBy(spec.pk).orderBy(
        F.col("source_lsn").desc_nulls_last(),
        F.col("source_tx_id").desc_nulls_last(),
        F.col("event_ts").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    return (
        valid_df.withColumn("_latest_rank", F.row_number().over(latest_window))
        .filter("_latest_rank = 1")
        .drop("_latest_rank")
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            *spec.data_columns,
            "is_deleted",
            "deleted_at",
            "event_id",
            "source_lsn",
            "source_tx_id",
            "event_ts",
            "kafka_offset",
            "ingestion_ts",
            "silver_updated_at",
        )
    )


def build_history(valid_df: DataFrame, spec: SilverEntitySpec) -> DataFrame:
    return (
        valid_df.withColumn("change_op", F.col("op"))
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("processing_date", F.to_date(F.col("ingestion_ts")))
        .select(
            *spec.data_columns,
            "change_op",
            "is_deleted",
            "event_id",
            "source_lsn",
            "source_tx_id",
            "event_ts",
            "kafka_offset",
            "ingestion_ts",
            "valid_from",
            "processing_date",
        )
    )


def build_quarantine(stale_df: DataFrame, spec: SilverEntitySpec) -> DataFrame:
    return (
        stale_df.withColumn("entity", F.lit(spec.entity))
        .withColumn("primary_key", F.col(spec.pk).cast("string"))
        .withColumn("reason", F.lit("STALE_SOURCE_LSN"))
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("processing_date", F.to_date(F.current_timestamp()))
        .select(
            "entity",
            "primary_key",
            "reason",
            "event_id",
            "topic",
            "kafka_partition",
            "kafka_offset",
            "op",
            "source_table",
            "source_lsn",
            "previous_max_source_lsn",
            "source_tx_id",
            "event_ts",
            "key_json",
            "before_json",
            "after_json",
            "source_json",
            "quarantined_at",
            "processing_date",
        )
    )


def rebuild_entity(spark, config: AppConfig, spec: SilverEntitySpec) -> tuple[int, int, int]:
    bronze_df = spark.table(f"{config.catalog_name}.bronze.{spec.bronze_table}")
    events_df = deduplicate_events(parse_entity_events(bronze_df, spec))
    valid_df, stale_df = split_stale_events(events_df, spec.pk)
    current_df = build_current(valid_df, spec)
    history_df = build_history(valid_df, spec)
    quarantine_df = build_quarantine(stale_df, spec)

    current_target = f"{config.catalog_name}.silver.{spec.current_table}"
    history_target = f"{config.catalog_name}.silver.{spec.history_table}"
    quarantine_target = f"{config.catalog_name}.silver.silver_quarantine_events"

    spark.sql(f"DELETE FROM {current_target}")
    spark.sql(f"DELETE FROM {history_target}")
    spark.sql(f"DELETE FROM {quarantine_target} WHERE entity = '{spec.entity}'")
    current_df.writeTo(current_target).append()
    history_df.writeTo(history_target).append()

    quarantine_count = quarantine_df.count()
    if quarantine_count:
        quarantine_df.writeTo(quarantine_target).append()

    return current_df.count(), history_df.count(), quarantine_count
