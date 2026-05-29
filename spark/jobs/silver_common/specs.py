from __future__ import annotations

from spark.jobs.silver_common.processor import SilverEntitySpec
from spark.jobs.silver_common.schemas import ORDER_SCHEMA, PAYMENT_SCHEMA, REFUND_SCHEMA


ORDERS_SPEC = SilverEntitySpec(
    entity="orders",
    pk="order_id",
    bronze_table="bronze_orders_cdc",
    current_table="silver_orders_current",
    history_table="silver_orders_history",
    schema=ORDER_SCHEMA,
    data_columns=[
        "order_id",
        "customer_id",
        "order_status",
        "currency",
        "total_amount",
        "ordered_at",
        "created_at",
        "updated_at",
    ],
)

PAYMENTS_SPEC = SilverEntitySpec(
    entity="payments",
    pk="payment_id",
    bronze_table="bronze_payments_cdc",
    current_table="silver_payments_current",
    history_table="silver_payments_history",
    schema=PAYMENT_SCHEMA,
    data_columns=[
        "payment_id",
        "order_id",
        "payment_status",
        "payment_method",
        "requested_amount",
        "approved_amount",
        "failure_code",
        "failure_message",
        "requested_at",
        "approved_at",
        "captured_at",
        "failed_at",
        "created_at",
        "updated_at",
    ],
)

REFUNDS_SPEC = SilverEntitySpec(
    entity="refunds",
    pk="refund_id",
    bronze_table="bronze_refunds_cdc",
    current_table="silver_refunds_current",
    history_table="silver_refunds_history",
    schema=REFUND_SCHEMA,
    data_columns=[
        "refund_id",
        "payment_id",
        "order_id",
        "refund_status",
        "refund_amount",
        "reason",
        "requested_at",
        "approved_at",
        "completed_at",
        "rejected_at",
        "created_at",
        "updated_at",
    ],
)

ALL_SPECS = [ORDERS_SPEC, PAYMENTS_SPEC, REFUNDS_SPEC]
