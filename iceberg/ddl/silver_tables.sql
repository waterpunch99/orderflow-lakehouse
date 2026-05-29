CREATE NAMESPACE IF NOT EXISTS lakehouse.silver;

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_orders_current (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    currency STRING,
    total_amount DECIMAL(12, 2),
    ordered_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    silver_updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_orders_history (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    currency STRING,
    total_amount DECIMAL(12, 2),
    ordered_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    change_op STRING,
    is_deleted BOOLEAN,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    valid_from TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_payments_current (
    payment_id BIGINT,
    order_id BIGINT,
    payment_status STRING,
    payment_method STRING,
    requested_amount DECIMAL(12, 2),
    approved_amount DECIMAL(12, 2),
    failure_code STRING,
    failure_message STRING,
    requested_at TIMESTAMP,
    approved_at TIMESTAMP,
    captured_at TIMESTAMP,
    failed_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    silver_updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_payments_history (
    payment_id BIGINT,
    order_id BIGINT,
    payment_status STRING,
    payment_method STRING,
    requested_amount DECIMAL(12, 2),
    approved_amount DECIMAL(12, 2),
    failure_code STRING,
    failure_message STRING,
    requested_at TIMESTAMP,
    approved_at TIMESTAMP,
    captured_at TIMESTAMP,
    failed_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    change_op STRING,
    is_deleted BOOLEAN,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    valid_from TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_refunds_current (
    refund_id BIGINT,
    payment_id BIGINT,
    order_id BIGINT,
    refund_status STRING,
    refund_amount DECIMAL(12, 2),
    reason STRING,
    requested_at TIMESTAMP,
    approved_at TIMESTAMP,
    completed_at TIMESTAMP,
    rejected_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    silver_updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_refunds_history (
    refund_id BIGINT,
    payment_id BIGINT,
    order_id BIGINT,
    refund_status STRING,
    refund_amount DECIMAL(12, 2),
    reason STRING,
    requested_at TIMESTAMP,
    approved_at TIMESTAMP,
    completed_at TIMESTAMP,
    rejected_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    change_op STRING,
    is_deleted BOOLEAN,
    event_id STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    kafka_offset BIGINT,
    ingestion_ts TIMESTAMP,
    valid_from TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_quarantine_events (
    entity STRING,
    primary_key STRING,
    reason STRING,
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    op STRING,
    source_table STRING,
    source_lsn BIGINT,
    previous_max_source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    key_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    quarantined_at TIMESTAMP,
    processing_date DATE
) USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES ('write.format.default' = 'parquet');
