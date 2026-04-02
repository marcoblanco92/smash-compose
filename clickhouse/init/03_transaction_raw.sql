CREATE TABLE IF NOT EXISTS smash_olap.transactions_raw
(
    customer_id       String,
    transaction_id    String,
    amount            Float64,
    merchant_cat      LowCardinality(String),
    channel           LowCardinality(String),
    counterpart_token Nullable(String),
    transaction_date  DateTime,
    ingested_at       DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(transaction_date)
    ORDER BY (customer_id, transaction_date)
    TTL transaction_date + INTERVAL 14 DAY;