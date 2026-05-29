from __future__ import annotations

from pathlib import Path


def execute_sql_file(spark, path: str) -> None:
    sql_text = Path(path).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    for statement in statements:
        spark.sql(statement)
