CREATE MATERIALIZED VIEW IF NOT EXISTS smash_olap.mv_to_daily
            TO smash_olap.daily_metrics AS
SELECT
    JSONExtractString(raw_json, 'after', 'customer_id') AS customer_id,
    toDate(substring(JSONExtractString(raw_json, 'after', 'transaction_date'), 1, 19), 'Europe/Rome') AS day,

    sumState(abs(JSONExtractFloat(raw_json, 'after', 'amount')))        AS txn_sum_state,
    countState(JSONExtractFloat(raw_json, 'after', 'amount'))           AS txn_count_state,
    maxState(abs(JSONExtractFloat(raw_json, 'after', 'amount')))        AS txn_max_state,
    avgState(abs(JSONExtractFloat(raw_json, 'after', 'amount')))        AS txn_avg_state,
    stddevPopState(abs(JSONExtractFloat(raw_json, 'after', 'amount')))  AS txn_stddev_state,

    -- Distribuzione categorie
    sumMapState(
        [JSONExtractString(raw_json, 'after', 'merchant_category')],
        [toFloat64(abs(JSONExtractFloat(raw_json, 'after', 'amount')))]
    ) AS cat_amounts_state,

    sumMapState(
        [JSONExtractString(raw_json, 'after', 'merchant_category')],
        [toUInt64(1)]
    ) AS cat_counts_state,

    -- Distribuzione canali (conteggio transazioni per canale)
    sumMapState(
        [JSONExtractString(raw_json, 'after', 'channel')],
        [toUInt64(1)]
    ) AS channel_counts_state,

    -- Income INBOUND: wire/instant con amount > 0
    sumStateIf(
        JSONExtractFloat(raw_json, 'after', 'amount'),
        JSONExtractString(raw_json, 'after', 'channel') IN ('wire', 'instant')
        AND JSONExtractFloat(raw_json, 'after', 'amount') > 0
    ) AS income_sum_state,

    uniqState(assumeNotNull(JSONExtractString(raw_json, 'after', 'counterpart'))) AS counterparts_hll

FROM smash_olap.stg_transactions_kafka
WHERE JSONExtractString(raw_json, 'op') != 'd'
GROUP BY customer_id, day;