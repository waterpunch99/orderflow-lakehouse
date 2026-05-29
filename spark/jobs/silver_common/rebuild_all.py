from __future__ import annotations

from spark.common.config import load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.silver_common.processor import rebuild_entity
from spark.jobs.silver_common.specs import ALL_SPECS
from spark.jobs.silver_common.sql import execute_sql_file


def main() -> None:
    logger = get_logger("silver-rebuild-all")
    config = load_config()
    spark = build_spark_session(config, "silver-rebuild-all")
    spark.sparkContext.setLogLevel("WARN")
    execute_sql_file(spark, "/opt/orderflow/iceberg/ddl/silver_tables.sql")

    for spec in ALL_SPECS:
        current_count, history_count, quarantine_count = rebuild_entity(spark, config, spec)
        logger.info(
            "entity=%s current_count=%s history_count=%s quarantine_count=%s",
            spec.entity,
            current_count,
            history_count,
            quarantine_count,
        )

    spark.stop()


if __name__ == "__main__":
    main()
