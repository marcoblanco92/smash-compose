DROP TABLE IF EXISTS smash_olap.mv_to_raw;

CREATE MATERIALIZED VIEW smash_olap.mv_to_raw
            TO smash_olap.transactions_raw AS
SELECT
    JSONExtractString(raw_json, 'after', 'customer_id') as customer_id,
    JSONExtractString(raw_json, 'after', 'transaction_id') as transaction_id,
    JSONExtractFloat(raw_json, 'after', 'amount') as amount,
    JSONExtractString(raw_json, 'after', 'merchant_category') as merchant_cat,
    JSONExtractString(raw_json, 'after', 'channel') as channel,
    JSONExtractString(raw_json, 'after', 'counterpart') as counterpart_token,
    toDateTime(substring(JSONExtractString(raw_json, 'after', 'transaction_date'), 1, 19)) as transaction_date,
    now() as ingested_at
FROM smash_olap.stg_transactions_kafka
WHERE JSONExtractString(raw_json, 'op') != 'd';