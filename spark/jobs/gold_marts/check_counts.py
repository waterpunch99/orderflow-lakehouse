from __future__ import annotations

from spark.common.config import load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.gold_marts.rebuild import GOLD_TABLES, _table
from spark.jobs.silver_common.sql import execute_sql_file


def main() -> None:
    logger = get_logger("gold-check-counts")
    config = load_config()
    spark = build_spark_session(config, "gold-check-counts")
    spark.sparkContext.setLogLevel("WARN")
    execute_sql_file(spark, "/opt/orderflow/iceberg/ddl/gold_tables.sql")

    for table_name in GOLD_TABLES:
        table = _table(config, "gold", table_name)
        logger.info("%s count=%s", table, spark.table(table).count())

    spark.stop()


if __name__ == "__main__":
    main()
