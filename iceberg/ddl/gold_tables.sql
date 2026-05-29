CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

CREATE TABLE IF NOT EXISTS lakehouse.gold.gold_daily_order_payment_summary (
    summary_date DATE,
    order_count BIGINT,
    paid_order_count BIGINT,
    payment_success_order_count BIGINT,
    payment_success_amount DECIMAL(18, 2),
    payment_failed_count BIGINT,
    payment_success_rate DOUBLE,
    refund_count BIGINT,
    refund_amount DECIMAL(18, 2),
    refund_rate DOUBLE,
    generated_at TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.gold_order_funnel_summary (
    summary_date DATE,
    created_order_count BIGINT,
    paid_order_count BIGINT,
    payment_completed_order_count BIGINT,
    cancelled_order_count BIGINT,
    refunded_order_count BIGINT,
    order_to_paid_rate DOUBLE,
    order_to_payment_completed_rate DOUBLE,
    order_to_refunded_rate DOUBLE,
    generated_at TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.gold_payment_failure_summary (
    failure_date DATE,
    payment_method STRING,
    failure_code STRING,
    failed_payment_count BIGINT,
    failed_order_count BIGINT,
    failed_requested_amount DECIMAL(18, 2),
    generated_at TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.gold_refund_summary (
    refund_date DATE,
    refund_status STRING,
    refund_count BIGINT,
    completed_refund_count BIGINT,
    refund_amount DECIMAL(18, 2),
    completed_refund_amount DECIMAL(18, 2),
    affected_order_count BIGINT,
    refund_rate DOUBLE,
    generated_at TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');
