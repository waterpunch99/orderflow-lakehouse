from __future__ import annotations

from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("order_status", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("ordered_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("requested_amount", DecimalType(12, 2), True),
        StructField("approved_amount", DecimalType(12, 2), True),
        StructField("failure_code", StringType(), True),
        StructField("failure_message", StringType(), True),
        StructField("requested_at", TimestampType(), True),
        StructField("approved_at", TimestampType(), True),
        StructField("captured_at", TimestampType(), True),
        StructField("failed_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

REFUND_SCHEMA = StructType(
    [
        StructField("refund_id", LongType(), True),
        StructField("payment_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("refund_status", StringType(), True),
        StructField("refund_amount", DecimalType(12, 2), True),
        StructField("reason", StringType(), True),
        StructField("requested_at", TimestampType(), True),
        StructField("approved_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("rejected_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
