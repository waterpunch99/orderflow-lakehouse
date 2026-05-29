CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_customers_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_products_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_orders_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_order_items_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_payments_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_refunds_cdc (
    event_id STRING,
    topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    op STRING,
    key_json STRING,
    value_json STRING,
    before_json STRING,
    after_json STRING,
    source_json STRING,
    transaction_json STRING,
    source_table STRING,
    source_lsn BIGINT,
    source_tx_id BIGINT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    processing_date DATE
)
USING iceberg
PARTITIONED BY (processing_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);
