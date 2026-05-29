from __future__ import annotations

import os
import sys

from spark.common.config import load_config
from spark.common.logging import get_logger
from spark.common.spark_session import build_spark_session
from quality.checks.result import CheckResult
from quality.checks.spark_checks import expect_equal, expect_zero, failed_checks, info_count
from spark.jobs.gold_marts.rebuild import GOLD_TABLES


SOURCE_TABLES = ["orders", "order_items", "payments", "refunds"]
SILVER_ENTITIES = ["orders", "payments", "refunds"]
BRONZE_TABLES = [
    "bronze_customers_cdc",
    "bronze_products_cdc",
    "bronze_orders_cdc",
    "bronze_order_items_cdc",
    "bronze_payments_cdc",
    "bronze_refunds_cdc",
]


def _postgres_options() -> dict[str, str]:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_INTERNAL_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "orderflow")
    user = os.getenv("POSTGRES_USER", "orderflow")
    password = os.getenv("POSTGRES_PASSWORD", "orderflow_pw")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _load_source_tables(spark) -> None:
    options = _postgres_options()
    for table in SOURCE_TABLES:
        (
            spark.read.format("jdbc")
            .option("url", options["url"])
            .option("dbtable", f"public.{table}")
            .option("user", options["user"])
            .option("password", options["password"])
            .option("driver", options["driver"])
            .load()
            .createOrReplaceTempView(f"src_{table}")
        )


def _register_lakehouse_views(spark, catalog: str) -> None:
    for entity in SILVER_ENTITIES:
        spark.table(f"{catalog}.silver.silver_{entity}_current").createOrReplaceTempView(f"silver_{entity}_current")
        spark.table(f"{catalog}.silver.silver_{entity}_history").createOrReplaceTempView(f"silver_{entity}_history")

    spark.table(f"{catalog}.silver.silver_quarantine_events").createOrReplaceTempView("silver_quarantine_events")

    for table in BRONZE_TABLES:
        spark.table(f"{catalog}.bronze.{table}").createOrReplaceTempView(table)

    for table in GOLD_TABLES:
        spark.table(f"{catalog}.gold.{table}").createOrReplaceTempView(table)


def _source_domain_checks(spark) -> list[CheckResult]:
    return [
        expect_zero(
            spark,
            "source.orders_total_amount_matches_order_items",
            """
            SELECT COUNT(*)
            FROM src_orders o
            LEFT JOIN (
                SELECT order_id, SUM(item_amount) AS item_total
                FROM src_order_items
                GROUP BY order_id
            ) i ON o.order_id = i.order_id
            WHERE o.total_amount <> COALESCE(i.item_total, 0)
            """,
            "orders.total_amount must equal sum(order_items.item_amount)",
        ),
        expect_zero(
            spark,
            "source.payments_approved_amount_not_greater_than_order_total",
            """
            SELECT COUNT(*)
            FROM src_payments p
            JOIN src_orders o ON p.order_id = o.order_id
            WHERE p.approved_amount IS NOT NULL
              AND p.approved_amount > o.total_amount
            """,
            "payments.approved_amount must be <= orders.total_amount",
        ),
        expect_zero(
            spark,
            "source.refunds_cumulative_amount_not_greater_than_payment_approved_amount",
            """
            SELECT COUNT(*)
            FROM (
                SELECT payment_id, SUM(refund_amount) AS total_refund_amount
                FROM src_refunds
                WHERE refund_status IN ('REQUESTED', 'APPROVED', 'COMPLETED')
                GROUP BY payment_id
            ) r
            JOIN src_payments p ON r.payment_id = p.payment_id
            WHERE r.total_refund_amount > COALESCE(p.approved_amount, 0)
            """,
            "cumulative refund_amount must be <= payment approved_amount",
        ),
    ]


def _silver_domain_checks(spark) -> list[CheckResult]:
    return [
        expect_zero(
            spark,
            "silver.payments_approved_amount_not_greater_than_order_total",
            """
            SELECT COUNT(*)
            FROM silver_payments_current p
            JOIN silver_orders_current o ON p.order_id = o.order_id
            WHERE p.is_deleted = false
              AND o.is_deleted = false
              AND p.approved_amount IS NOT NULL
              AND p.approved_amount > o.total_amount
            """,
            "Silver payments.approved_amount must be <= Silver orders.total_amount",
        ),
        expect_zero(
            spark,
            "silver.refunds_cumulative_amount_not_greater_than_payment_approved_amount",
            """
            SELECT COUNT(*)
            FROM (
                SELECT payment_id, SUM(refund_amount) AS total_refund_amount
                FROM silver_refunds_current
                WHERE is_deleted = false
                  AND refund_status IN ('REQUESTED', 'APPROVED', 'COMPLETED')
                GROUP BY payment_id
            ) r
            JOIN silver_payments_current p ON r.payment_id = p.payment_id
            WHERE p.is_deleted = false
              AND r.total_refund_amount > COALESCE(p.approved_amount, 0)
            """,
            "Silver cumulative refund_amount must be <= approved_amount",
        ),
        expect_zero(
            spark,
            "silver.paid_orders_have_captured_payment",
            """
            SELECT COUNT(*)
            FROM silver_orders_current o
            LEFT ANTI JOIN (
                SELECT DISTINCT order_id
                FROM silver_payments_current
                WHERE is_deleted = false
                  AND payment_status = 'CAPTURED'
            ) p ON o.order_id = p.order_id
            WHERE o.is_deleted = false
              AND o.order_status = 'PAID'
            """,
            "PAID orders must have at least one CAPTURED payment",
        ),
        expect_zero(
            spark,
            "silver.refunded_orders_have_completed_refund",
            """
            SELECT COUNT(*)
            FROM silver_orders_current o
            LEFT ANTI JOIN (
                SELECT DISTINCT order_id
                FROM silver_refunds_current
                WHERE is_deleted = false
                  AND refund_status = 'COMPLETED'
            ) r ON o.order_id = r.order_id
            WHERE o.is_deleted = false
              AND o.order_status = 'REFUNDED'
            """,
            "REFUNDED orders must have at least one COMPLETED refund",
        ),
    ]


def _row_count_reconciliation_checks(spark) -> list[CheckResult]:
    results: list[CheckResult] = []
    for entity in SILVER_ENTITIES:
        source_count = spark.table(f"src_{entity}").count()
        silver_count = spark.sql(
            f"SELECT COUNT(*) FROM silver_{entity}_current WHERE is_deleted = false"
        ).first()[0]
        results.append(
            expect_equal(
                f"reconciliation.source_{entity}_active_count_equals_silver_current",
                int(source_count),
                int(silver_count),
                f"PostgreSQL {entity} row count vs non-deleted silver_{entity}_current row count",
            )
        )

        delete_event_count = spark.sql(
            f"SELECT COUNT(*) FROM bronze_{entity}_cdc WHERE op = 'd'"
        ).first()[0]
        deleted_current_count = spark.sql(
            f"SELECT COUNT(*) FROM silver_{entity}_current WHERE is_deleted = true"
        ).first()[0]
        results.append(
            expect_equal(
                f"reconciliation.{entity}_delete_events_match_silver_soft_deletes",
                int(delete_event_count),
                int(deleted_current_count),
                f"Bronze {entity} delete event count vs Silver soft-deleted row count",
            )
        )
    return results


def _metadata_checks(spark) -> list[CheckResult]:
    duplicate_checks = [
        expect_zero(
            spark,
            f"bronze.{table}.duplicate_event_id",
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM {table}
                WHERE event_id IS NOT NULL
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) d
            """,
            f"{table} event_id must be unique",
        )
        for table in BRONZE_TABLES
    ]

    return duplicate_checks + [
        info_count(
            spark,
            "silver.quarantine_event_count",
            "SELECT COUNT(*) FROM silver_quarantine_events",
            "quarantine events are reported for operational visibility",
        )
    ]


def _gold_presence_checks(spark) -> list[CheckResult]:
    return [
        info_count(
            spark,
            f"gold.{table}.row_count",
            f"SELECT COUNT(*) FROM {table}",
            "Gold mart row count is reported after rebuild",
        )
        for table in GOLD_TABLES
    ]


def run_checks(spark, catalog: str) -> list[CheckResult]:
    _load_source_tables(spark)
    _register_lakehouse_views(spark, catalog)
    results: list[CheckResult] = []
    results.extend(_source_domain_checks(spark))
    results.extend(_silver_domain_checks(spark))
    results.extend(_row_count_reconciliation_checks(spark))
    results.extend(_metadata_checks(spark))
    results.extend(_gold_presence_checks(spark))
    return results


def main() -> None:
    logger = get_logger("quality-checks")
    config = load_config()
    spark = build_spark_session(config, "quality-checks")
    spark.sparkContext.setLogLevel("WARN")

    results = run_checks(spark, config.catalog_name)
    for result in results:
        logger.info(result.format())

    failures = failed_checks(results)
    logger.info("quality_check_summary total=%s failed=%s", len(results), len(failures))
    spark.stop()

    if failures:
        for failure in failures:
            print(failure.format(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
