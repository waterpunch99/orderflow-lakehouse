from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark.common.config import AppConfig


def kafka_topic_pattern(config: AppConfig) -> str:
    if not config.kafka.cdc_topics:
        raise ValueError("cdc_topics must not be empty")
    return ",".join(config.kafka.cdc_topics)


def read_kafka_stream(spark: SparkSession, config: AppConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", kafka_topic_pattern(config))
        .option("startingOffsets", config.kafka.starting_offsets)
        .option("failOnDataLoss", config.kafka.fail_on_data_loss)
        .load()
    )


def read_kafka_batch(spark: SparkSession, config: AppConfig) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", kafka_topic_pattern(config))
        .option("startingOffsets", config.kafka.starting_offsets)
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", config.kafka.fail_on_data_loss)
        .load()
    )
