from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


DEBEZIUM_SOURCE_SCHEMA = StructType(
    [
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True),
    ]
)

DEBEZIUM_TRANSACTION_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("total_order", LongType(), True),
        StructField("data_collection_order", LongType(), True),
    ]
)


def json_string(column: Column) -> Column:
    return F.to_json(column)


def parse_debezium_json(df: DataFrame) -> DataFrame:
    value_json = F.col("value").cast("string")
    key_json = F.col("key").cast("string")
    source_json = F.get_json_object(value_json, "$.source")
    source = F.from_json(source_json, DEBEZIUM_SOURCE_SCHEMA)

    return (
        df.withColumn("key_json", key_json)
        .withColumn("value_json", value_json)
        .withColumn("op", F.get_json_object(value_json, "$.op"))
        .withColumn("before_json", F.get_json_object(value_json, "$.before"))
        .withColumn("after_json", F.get_json_object(value_json, "$.after"))
        .withColumn("source_json", source_json)
        .withColumn("transaction_json", F.get_json_object(value_json, "$.transaction"))
        .withColumn("source", source)
        .withColumn("source_table", F.col("source.table"))
        .withColumn("source_lsn", F.col("source.lsn"))
        .withColumn("source_tx_id", F.col("source.txId"))
        .withColumn("event_ts", (F.get_json_object(value_json, "$.ts_ms").cast("double") / F.lit(1000)).cast("timestamp"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def with_kafka_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
    )


def with_event_id(df: DataFrame) -> DataFrame:
    event_key = F.concat_ws(
        ":",
        F.col("topic"),
        F.col("kafka_partition").cast("string"),
        F.col("kafka_offset").cast("string"),
    )
    return df.withColumn("event_id", F.sha2(event_key, 256))
