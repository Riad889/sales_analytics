CREATE OR REPLACE TABLE IF NOT EXISTS sales_analytics.sales (
    customer_id STRING,
    customer_email STRING,
    customer_country STRING,
    customer_city STRING,
    customer_segment STRING,
    is_returning_customer BOOLEAN,

    product_id STRING,
    product_name STRING,
    product_category STRING,
    base_price DOUBLE,
    quantity INT,

    sales_channel STRING,
    campaign STRING,
    traffic_source STRING,
    device_type STRING,

    unit_price DOUBLE,
    subtotal DOUBLE,
    discount_rate DOUBLE,
    discount_amount DOUBLE,
    tax_amount DOUBLE,
    shipping_fee DOUBLE,
    total_amount DOUBLE,

    payment_method STRING,
    payment_status STRING,

    order_date_ts TIMESTAMP,
    confirmation_date_ts TIMESTAMP,
    order_status STRING,

    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
using iceberg
PARTITION BY (days(order_date_ts)) 
TBLPROPERTIES (
    'format-version' = '2',

    -- Snapshot retention
    'history.expire.max-snapshot-age-ms' = '604800000',
    'history.expire.min-snapshots-to-keep' = '10',

    -- Write optimization
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd',

    -- Metadata / maintenance
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);