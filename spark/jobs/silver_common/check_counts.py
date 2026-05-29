from __future__ import annotations

from spark.common.config import load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.silver_common.specs import ALL_SPECS


def main() -> None:
    logger = get_logger("silver-check-counts")
    config = load_config()
    spark = build_spark_session(config, "silver-check-counts")
    spark.sparkContext.setLogLevel("WARN")

    for spec in ALL_SPECS:
        current = f"{config.catalog_name}.silver.{spec.current_table}"
        history = f"{config.catalog_name}.silver.{spec.history_table}"
        logger.info("%s count=%s", current, spark.table(current).count())
        logger.info("%s count=%s", history, spark.table(history).count())

    quarantine = f"{config.catalog_name}.silver.silver_quarantine_events"
    logger.info("%s count=%s", quarantine, spark.table(quarantine).count())
    spark.stop()


if __name__ == "__main__":
    main()
