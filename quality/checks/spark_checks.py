from __future__ import annotations

from collections.abc import Iterable

from quality.checks.result import CheckResult


def scalar_count(spark, sql: str) -> int:
    row = spark.sql(sql).first()
    if row is None:
        return 0
    return int(row[0] or 0)


def expect_zero(spark, name: str, sql: str, details: str) -> CheckResult:
    count = scalar_count(spark, sql)
    return CheckResult(
        name=name,
        status="PASS" if count == 0 else "FAIL",
        observed_value=count,
        expected="0",
        details=details,
    )


def expect_equal(name: str, left: int, right: int, details: str) -> CheckResult:
    return CheckResult(
        name=name,
        status="PASS" if left == right else "FAIL",
        observed_value=f"{left} vs {right}",
        expected="equal",
        details=details,
    )


def info_count(spark, name: str, sql: str, details: str) -> CheckResult:
    return CheckResult(
        name=name,
        status="INFO",
        observed_value=scalar_count(spark, sql),
        expected="reported",
        details=details,
    )


def failed_checks(results: Iterable[CheckResult]) -> list[CheckResult]:
    return [result for result in results if result.failed]
