from __future__ import annotations

from spark.common.config import load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.bronze_ingestion.main import TABLE_BY_SOURCE


def main() -> None:
    logger = get_logger("bronze_check_counts")
    config = load_config()
    spark = build_spark_session(config, "bronze-check-counts")
    spark.sparkContext.setLogLevel("WARN")

    for source_table, bronze_table in TABLE_BY_SOURCE.items():
        full_name = f"{config.catalog_name}.bronze.{bronze_table}"
        table_df = spark.table(full_name)
        count = table_df.count()
        distinct_event_ids = table_df.select("event_id").distinct().count()
        null_event_ids = table_df.filter("event_id is null").count()
        logger.info(
            "%s source_table=%s count=%s distinct_event_ids=%s null_event_ids=%s",
            full_name,
            source_table,
            count,
            distinct_event_ids,
            null_event_ids,
        )

    spark.stop()


if __name__ == "__main__":
    main()
