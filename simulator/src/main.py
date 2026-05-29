from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DatabaseConfig:
    compose_service: str
    dbname: str
    user: str


@dataclass(frozen=True)
class SimulationConfig:
    batch_id_prefix: str
    sleep_seconds: float
    run_success_flow: bool
    run_failure_flow: bool
    run_refund_flow: bool
    run_delete_flow: bool
    run_rapid_update_flow: bool


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    simulation: SimulationConfig


def parse_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value.strip("'\"")


def load_simple_yaml(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    current_section: str | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if not raw_line.startswith(" ") and raw_line.rstrip().endswith(":"):
            current_section = raw_line.strip()[:-1]
            result[current_section] = {}
            continue
        if current_section is None or ":" not in raw_line:
            raise ValueError(f"unsupported config line: {raw_line}")
        key, value = raw_line.strip().split(":", 1)
        result[current_section][key.strip()] = parse_scalar(value.strip())

    return result


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def parse_config(config_path: Path, env_path: Path | None) -> AppConfig:
    if env_path is not None:
        load_dotenv(env_path)

    raw = load_simple_yaml(config_path)
    database = raw.get("database", {})
    simulation = raw.get("simulation", {})

    db_config = DatabaseConfig(
        compose_service=str(database.get("compose_service", "postgres")),
        dbname=str(os.getenv("POSTGRES_DB", database.get("dbname", "orderflow"))),
        user=str(os.getenv("POSTGRES_USER", database.get("user", "orderflow"))),
    )
    sim_config = SimulationConfig(
        batch_id_prefix=str(simulation.get("batch_id_prefix", "sim")),
        sleep_seconds=float(simulation.get("sleep_seconds", 0.2)),
        run_success_flow=bool(simulation.get("run_success_flow", True)),
        run_failure_flow=bool(simulation.get("run_failure_flow", True)),
        run_refund_flow=bool(simulation.get("run_refund_flow", True)),
        run_delete_flow=bool(simulation.get("run_delete_flow", True)),
        run_rapid_update_flow=bool(simulation.get("run_rapid_update_flow", True)),
    )
    return AppConfig(database=db_config, simulation=sim_config)


def now_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sleep_sql(seconds: float) -> str:
    if seconds <= 0:
        return ""
    return f"select pg_sleep({seconds});\n"


def tx(*statements: str, sleep_seconds: float) -> str:
    body = "\n".join(statement.strip() for statement in statements if statement.strip())
    return f"begin;\n{body}\ncommit;\n{sleep_sql(sleep_seconds)}"


class SqlScenarioBuilder:
    def __init__(self, run_id: str, config: SimulationConfig) -> None:
        self.run_id = run_id
        self.config = config

    def build(self) -> str:
        parts = [
            "\\set ON_ERROR_STOP on",
            f"\\echo simulator run_id={self.run_id}",
            "create temp table if not exists _sim_ids (key text primary key, id bigint) on commit preserve rows;",
        ]

        parts.extend(self.master_data("success"))
        if self.config.run_success_flow:
            parts.extend(self.order_flow("success", final_state="paid"))

        if self.config.run_failure_flow:
            parts.extend(self.master_data("failure"))
            parts.extend(self.order_flow("failure", final_state="failed"))

        if self.config.run_refund_flow:
            parts.extend(self.master_data("refund"))
            parts.extend(self.order_flow("refund", final_state="refunded"))

        if self.config.run_rapid_update_flow:
            parts.extend(self.master_data("rapid"))
            parts.extend(self.rapid_updates("rapid"))

        if self.config.run_delete_flow:
            parts.extend(self.master_data("delete"))
            parts.extend(self.delete_flow("delete"))

        parts.append("\\echo simulation completed")
        return "\n".join(parts)

    def key(self, label: str, name: str) -> str:
        return f"{label}.{name}"

    def master_data(self, label: str) -> list[str]:
        run = sql_literal(self.run_id)
        customer_key = sql_literal(self.key(label, "customer"))
        product_a_key = sql_literal(self.key(label, "product_a"))
        product_b_key = sql_literal(self.key(label, "product_b"))
        label_lit = sql_literal(label)
        sleep = self.config.sleep_seconds

        return [
            tx(
                f"""
                with ins as (
                    insert into customers (customer_name, email, phone, customer_status)
                    values (
                        'Sim Customer ' || {run} || ' ' || {label_lit},
                        {run} || '.' || {label_lit} || '@example.com',
                        '+82-10-2000-0000',
                        'ACTIVE'
                    )
                    returning customer_id
                )
                insert into _sim_ids (key, id)
                select {customer_key}, customer_id from ins;
                """,
                sleep_seconds=sleep,
            ),
            tx(
                f"""
                with product_rows(suffix, product_name, unit_price) as (
                    values
                        ('A', 'Sim Product ' || {label_lit} || ' A', 7000.00::numeric),
                        ('B', 'Sim Product ' || {label_lit} || ' B', 13000.00::numeric)
                ),
                ins as (
                    insert into products (sku, product_name, category, unit_price, product_status)
                    select 'SIM-' || {run} || '-' || {label_lit} || '-' || suffix,
                           product_name,
                           'simulation',
                           unit_price,
                           'ACTIVE'
                    from product_rows
                    returning product_id, sku
                )
                insert into _sim_ids (key, id)
                select case
                           when sku like '%-A' then {product_a_key}
                           else {product_b_key}
                       end,
                       product_id
                from ins;
                """,
                sleep_seconds=sleep,
            ),
        ]

    def order_flow(self, label: str, final_state: str) -> list[str]:
        sleep = self.config.sleep_seconds
        order_key = sql_literal(self.key(label, "order"))
        payment_key = sql_literal(self.key(label, "payment"))
        refund_key = sql_literal(self.key(label, "refund"))
        customer_key = sql_literal(self.key(label, "customer"))
        product_a_key = sql_literal(self.key(label, "product_a"))
        product_b_key = sql_literal(self.key(label, "product_b"))

        order_tx = tx(
            f"""
            with selected_products as (
                select p.product_id, p.unit_price
                from products p
                join _sim_ids ids on ids.id = p.product_id
                where ids.key in ({product_a_key}, {product_b_key})
            ),
            order_ins as (
                insert into orders (customer_id, order_status, currency, total_amount)
                select customer_ids.id,
                       'PENDING_PAYMENT',
                       'KRW',
                       (select sum(unit_price) from selected_products)
                from _sim_ids customer_ids
                where customer_ids.key = {customer_key}
                returning order_id
            ),
            remember_order as (
                insert into _sim_ids (key, id)
                select {order_key}, order_id from order_ins
                returning id
            )
            insert into order_items (order_id, product_id, quantity, unit_price, item_amount)
            select remember_order.id, product_id, 1, unit_price, unit_price
            from remember_order
            cross join selected_products;
            """,
            sleep_seconds=sleep,
        )

        payment_request = tx(
            f"""
            with ins as (
                insert into payments (order_id, payment_status, payment_method, requested_amount)
                select o.id, 'REQUESTED', 'CARD', orders.total_amount
                from _sim_ids o
                join orders on orders.order_id = o.id
                where o.key = {order_key}
                returning payment_id
            )
            insert into _sim_ids (key, id)
            select {payment_key}, payment_id from ins;
            """,
            sleep_seconds=sleep,
        )

        statements = [order_tx, payment_request]

        if final_state == "failed":
            statements.extend(
                [
                    tx(
                        f"""
                        update payments
                        set payment_status = 'FAILED',
                            failure_code = 'CARD_DECLINED',
                            failure_message = 'Simulated card authorization failure',
                            failed_at = now()
                        where payment_id = (select id from _sim_ids where key = {payment_key});
                        """,
                        sleep_seconds=sleep,
                    ),
                    tx(
                        f"""
                        update orders
                        set order_status = 'CANCELLED'
                        where order_id = (select id from _sim_ids where key = {order_key});
                        """,
                        sleep_seconds=sleep,
                    ),
                ]
            )
            return statements

        statements.extend(self.payment_success_updates(order_key, payment_key))

        if final_state == "refunded":
            statements.extend(
                [
                    tx(
                        f"""
                        update orders
                        set order_status = 'REFUND_REQUESTED'
                        where order_id = (select id from _sim_ids where key = {order_key});

                        with ins as (
                            insert into refunds (payment_id, order_id, refund_status, refund_amount, reason)
                            select p.id,
                                   o.id,
                                   'REQUESTED',
                                   payments.approved_amount,
                                   'Simulated customer refund request'
                            from _sim_ids p
                            join _sim_ids o on o.key = {order_key}
                            join payments on payments.payment_id = p.id
                            where p.key = {payment_key}
                            returning refund_id
                        )
                        insert into _sim_ids (key, id)
                        select {refund_key}, refund_id from ins;
                        """,
                        sleep_seconds=sleep,
                    ),
                    tx(
                        f"""
                        update refunds
                        set refund_status = 'COMPLETED',
                            approved_at = now(),
                            completed_at = now()
                        where refund_id = (select id from _sim_ids where key = {refund_key});

                        update orders
                        set order_status = 'REFUNDED'
                        where order_id = (select id from _sim_ids where key = {order_key});
                        """,
                        sleep_seconds=sleep,
                    ),
                ]
            )

        return statements

    def payment_success_updates(self, order_key: str, payment_key: str) -> list[str]:
        sleep = self.config.sleep_seconds
        return [
            tx(
                f"""
                update payments
                set payment_status = 'APPROVED',
                    approved_amount = requested_amount,
                    approved_at = now()
                where payment_id = (select id from _sim_ids where key = {payment_key});
                """,
                sleep_seconds=sleep,
            ),
            tx(
                f"""
                update payments
                set payment_status = 'CAPTURED',
                    captured_at = now()
                where payment_id = (select id from _sim_ids where key = {payment_key});
                """,
                sleep_seconds=sleep,
            ),
            tx(
                f"""
                update orders
                set order_status = 'PAID'
                where order_id = (select id from _sim_ids where key = {order_key});
                """,
                sleep_seconds=sleep,
            ),
        ]

    def rapid_updates(self, label: str) -> list[str]:
        customer_key = sql_literal(self.key(label, "customer"))
        statuses = ["SUSPENDED", "ACTIVE", "SUSPENDED", "ACTIVE"]
        statements = []
        for index, status in enumerate(statuses, start=1):
            statements.append(
                tx(
                    f"""
                    update customers
                    set customer_status = {sql_literal(status)},
                        phone = {sql_literal(f"+82-10-2999-00{index:02d}")}
                    where customer_id = (select id from _sim_ids where key = {customer_key});
                    """,
                    sleep_seconds=self.config.sleep_seconds,
                )
            )
        return statements

    def delete_flow(self, label: str) -> list[str]:
        sleep = self.config.sleep_seconds
        order_key = sql_literal(self.key(label, "order"))
        customer_key = sql_literal(self.key(label, "customer"))
        product_a_key = sql_literal(self.key(label, "product_a"))

        create_order = tx(
            f"""
            with product_row as (
                select p.product_id, p.unit_price
                from products p
                join _sim_ids ids on ids.id = p.product_id
                where ids.key = {product_a_key}
            ),
            order_ins as (
                insert into orders (customer_id, order_status, currency, total_amount)
                select customer_ids.id,
                       'CREATED',
                       'KRW',
                       (select unit_price from product_row)
                from _sim_ids customer_ids
                where customer_ids.key = {customer_key}
                returning order_id
            ),
            remember_order as (
                insert into _sim_ids (key, id)
                select {order_key}, order_id from order_ins
                returning id
            )
            insert into order_items (order_id, product_id, quantity, unit_price, item_amount)
            select remember_order.id, product_id, 1, unit_price, unit_price
            from remember_order
            cross join product_row;
            """,
            sleep_seconds=sleep,
        )

        delete_rows = tx(
            f"""
            delete from order_items
            where order_id = (select id from _sim_ids where key = {order_key});

            delete from orders
            where order_id = (select id from _sim_ids where key = {order_key});

            delete from products
            where product_id = (select id from _sim_ids where key = {product_a_key});

            delete from customers
            where customer_id = (select id from _sim_ids where key = {customer_key});
            """,
            sleep_seconds=sleep,
        )
        return [create_order, delete_rows]


def run_psql(config: AppConfig, sql: str) -> None:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        config.database.compose_service,
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        config.database.user,
        "-d",
        config.database.dbname,
    ]
    subprocess.run(command, input=sql, text=True, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate PostgreSQL source transactions for CDC testing.")
    parser.add_argument("--config", default="simulator/config.yaml", help="Path to simulator config YAML.")
    parser.add_argument("--env-file", default=".env", help="Optional .env file. Use an empty value to skip loading.")
    parser.add_argument(
        "--run-id",
        default=None,
        help="Unique run identifier. Defaults to <batch_id_prefix>-<utc timestamp>.",
    )
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print generated SQL without executing it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = parse_config(Path(args.config), Path(args.env_file) if args.env_file else None)
    run_id = args.run_id or f"{config.simulation.batch_id_prefix}-{now_token()}"
    sql = SqlScenarioBuilder(run_id=run_id, config=config.simulation).build()

    if args.print_sql:
        print(sql)
        return

    run_psql(config, sql)


if __name__ == "__main__":
    main()
