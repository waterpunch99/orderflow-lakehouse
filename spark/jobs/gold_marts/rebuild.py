from __future__ import annotations

from dataclasses import dataclass

from spark.common.config import AppConfig, load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from spark.jobs.silver_common.sql import execute_sql_file


@dataclass(frozen=True)
class MartResult:
    table: str
    row_count: int


GOLD_TABLES = [
    "gold_daily_order_payment_summary",
    "gold_order_funnel_summary",
    "gold_payment_failure_summary",
    "gold_refund_summary",
]


def _table(config: AppConfig, namespace: str, table_name: str) -> str:
    return f"{config.catalog_name}.{namespace}.{table_name}"


def _delete_gold_tables(spark, config: AppConfig) -> None:
    for table_name in GOLD_TABLES:
        spark.sql(f"DELETE FROM {_table(config, 'gold', table_name)}")


def _insert_daily_order_payment_summary(spark, config: AppConfig) -> None:
    orders = _table(config, "silver", "silver_orders_current")
    payments = _table(config, "silver", "silver_payments_current")
    refunds = _table(config, "silver", "silver_refunds_current")
    target = _table(config, "gold", "gold_daily_order_payment_summary")

    spark.sql(
        f"""
        INSERT INTO {target}
        WITH active_orders AS (
            SELECT
                order_id,
                order_status,
                total_amount,
                CAST(COALESCE(ordered_at, created_at, updated_at, silver_updated_at) AS DATE) AS summary_date
            FROM {orders}
            WHERE is_deleted = false
        ),
        payment_by_order AS (
            SELECT
                order_id,
                COUNT(*) AS payment_count,
                SUM(CASE WHEN payment_status = 'CAPTURED' THEN 1 ELSE 0 END) AS success_payment_count,
                SUM(CASE WHEN payment_status = 'CAPTURED' THEN COALESCE(approved_amount, requested_amount, CAST(0 AS DECIMAL(12, 2))) ELSE CAST(0 AS DECIMAL(12, 2)) END) AS success_amount,
                SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payment_count
            FROM {payments}
            WHERE is_deleted = false
            GROUP BY order_id
        ),
        refund_by_order AS (
            SELECT
                order_id,
                COUNT(*) AS refund_count,
                SUM(CASE WHEN refund_status = 'COMPLETED' THEN COALESCE(refund_amount, CAST(0 AS DECIMAL(12, 2))) ELSE CAST(0 AS DECIMAL(12, 2)) END) AS refund_amount
            FROM {refunds}
            WHERE is_deleted = false
            GROUP BY order_id
        )
        SELECT
            o.summary_date,
            COUNT(*) AS order_count,
            SUM(CASE WHEN o.order_status IN ('PAID', 'REFUNDED') THEN 1 ELSE 0 END) AS paid_order_count,
            SUM(CASE WHEN COALESCE(p.success_payment_count, 0) > 0 THEN 1 ELSE 0 END) AS payment_success_order_count,
            CAST(COALESCE(SUM(COALESCE(p.success_amount, CAST(0 AS DECIMAL(12, 2)))), CAST(0 AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS payment_success_amount,
            SUM(COALESCE(p.failed_payment_count, 0)) AS payment_failed_count,
            CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(SUM(CASE WHEN COALESCE(p.success_payment_count, 0) > 0 THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
            END AS payment_success_rate,
            SUM(COALESCE(r.refund_count, 0)) AS refund_count,
            CAST(COALESCE(SUM(COALESCE(r.refund_amount, CAST(0 AS DECIMAL(12, 2)))), CAST(0 AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS refund_amount,
            CASE WHEN SUM(CASE WHEN COALESCE(p.success_payment_count, 0) > 0 THEN 1 ELSE 0 END) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(SUM(CASE WHEN COALESCE(r.refund_count, 0) > 0 THEN 1 ELSE 0 END) AS DOUBLE)
                      / CAST(SUM(CASE WHEN COALESCE(p.success_payment_count, 0) > 0 THEN 1 ELSE 0 END) AS DOUBLE)
            END AS refund_rate,
            current_timestamp() AS generated_at,
            current_date() AS processing_date
        FROM active_orders o
        LEFT JOIN payment_by_order p ON o.order_id = p.order_id
        LEFT JOIN refund_by_order r ON o.order_id = r.order_id
        WHERE o.summary_date IS NOT NULL
        GROUP BY o.summary_date
        """
    )


def _insert_order_funnel_summary(spark, config: AppConfig) -> None:
    orders = _table(config, "silver", "silver_orders_current")
    payments = _table(config, "silver", "silver_payments_current")
    target = _table(config, "gold", "gold_order_funnel_summary")

    spark.sql(
        f"""
        INSERT INTO {target}
        WITH active_orders AS (
            SELECT
                order_id,
                order_status,
                CAST(COALESCE(ordered_at, created_at, updated_at, silver_updated_at) AS DATE) AS summary_date
            FROM {orders}
            WHERE is_deleted = false
        ),
        payment_by_order AS (
            SELECT
                order_id,
                MAX(CASE WHEN payment_status = 'CAPTURED' THEN 1 ELSE 0 END) AS has_captured_payment
            FROM {payments}
            WHERE is_deleted = false
            GROUP BY order_id
        )
        SELECT
            o.summary_date,
            COUNT(*) AS created_order_count,
            SUM(CASE WHEN o.order_status IN ('PAID', 'REFUNDED') THEN 1 ELSE 0 END) AS paid_order_count,
            SUM(COALESCE(p.has_captured_payment, 0)) AS payment_completed_order_count,
            SUM(CASE WHEN o.order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_order_count,
            SUM(CASE WHEN o.order_status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_order_count,
            CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(SUM(CASE WHEN o.order_status IN ('PAID', 'REFUNDED') THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
            END AS order_to_paid_rate,
            CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(SUM(COALESCE(p.has_captured_payment, 0)) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
            END AS order_to_payment_completed_rate,
            CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(SUM(CASE WHEN o.order_status = 'REFUNDED' THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
            END AS order_to_refunded_rate,
            current_timestamp() AS generated_at,
            current_date() AS processing_date
        FROM active_orders o
        LEFT JOIN payment_by_order p ON o.order_id = p.order_id
        WHERE o.summary_date IS NOT NULL
        GROUP BY o.summary_date
        """
    )


def _insert_payment_failure_summary(spark, config: AppConfig) -> None:
    payments = _table(config, "silver", "silver_payments_current")
    target = _table(config, "gold", "gold_payment_failure_summary")

    spark.sql(
        f"""
        INSERT INTO {target}
        SELECT
            CAST(COALESCE(failed_at, updated_at, silver_updated_at) AS DATE) AS failure_date,
            COALESCE(payment_method, 'UNKNOWN') AS payment_method,
            COALESCE(failure_code, 'UNKNOWN') AS failure_code,
            COUNT(*) AS failed_payment_count,
            COUNT(DISTINCT order_id) AS failed_order_count,
            CAST(COALESCE(SUM(COALESCE(requested_amount, CAST(0 AS DECIMAL(12, 2)))), CAST(0 AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS failed_requested_amount,
            current_timestamp() AS generated_at,
            current_date() AS processing_date
        FROM {payments}
        WHERE is_deleted = false
          AND payment_status = 'FAILED'
          AND CAST(COALESCE(failed_at, updated_at, silver_updated_at) AS DATE) IS NOT NULL
        GROUP BY
            CAST(COALESCE(failed_at, updated_at, silver_updated_at) AS DATE),
            COALESCE(payment_method, 'UNKNOWN'),
            COALESCE(failure_code, 'UNKNOWN')
        """
    )


def _insert_refund_summary(spark, config: AppConfig) -> None:
    orders = _table(config, "silver", "silver_orders_current")
    refunds = _table(config, "silver", "silver_refunds_current")
    target = _table(config, "gold", "gold_refund_summary")

    spark.sql(
        f"""
        INSERT INTO {target}
        WITH active_orders AS (
            SELECT COUNT(*) AS order_count
            FROM {orders}
            WHERE is_deleted = false
        ),
        active_refunds AS (
            SELECT
                refund_status,
                order_id,
                refund_amount,
                CAST(COALESCE(completed_at, approved_at, requested_at, updated_at, silver_updated_at) AS DATE) AS refund_date
            FROM {refunds}
            WHERE is_deleted = false
        )
        SELECT
            r.refund_date,
            r.refund_status,
            COUNT(*) AS refund_count,
            SUM(CASE WHEN r.refund_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_refund_count,
            CAST(COALESCE(SUM(COALESCE(r.refund_amount, CAST(0 AS DECIMAL(12, 2)))), CAST(0 AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS refund_amount,
            CAST(COALESCE(SUM(CASE WHEN r.refund_status = 'COMPLETED' THEN COALESCE(r.refund_amount, CAST(0 AS DECIMAL(12, 2))) ELSE CAST(0 AS DECIMAL(12, 2)) END), CAST(0 AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS completed_refund_amount,
            COUNT(DISTINCT r.order_id) AS affected_order_count,
            CASE WHEN MAX(o.order_count) = 0 THEN CAST(0 AS DOUBLE)
                 ELSE CAST(COUNT(DISTINCT r.order_id) AS DOUBLE) / CAST(MAX(o.order_count) AS DOUBLE)
            END AS refund_rate,
            current_timestamp() AS generated_at,
            current_date() AS processing_date
        FROM active_refunds r
        CROSS JOIN active_orders o
        WHERE r.refund_date IS NOT NULL
        GROUP BY r.refund_date, r.refund_status
        """
    )


def rebuild_gold_marts(spark, config: AppConfig) -> list[MartResult]:
    execute_sql_file(spark, "/opt/orderflow/iceberg/ddl/gold_tables.sql")
    _delete_gold_tables(spark, config)
    _insert_daily_order_payment_summary(spark, config)
    _insert_order_funnel_summary(spark, config)
    _insert_payment_failure_summary(spark, config)
    _insert_refund_summary(spark, config)

    return [
        MartResult(table=table_name, row_count=spark.table(_table(config, "gold", table_name)).count())
        for table_name in GOLD_TABLES
    ]


def main() -> None:
    logger = get_logger("gold-rebuild")
    config = load_config()
    spark = build_spark_session(config, "gold-rebuild")
    spark.sparkContext.setLogLevel("WARN")

    for result in rebuild_gold_marts(spark, config):
        logger.info("%s count=%s", _table(config, "gold", result.table), result.row_count)

    spark.stop()


if __name__ == "__main__":
    main()
