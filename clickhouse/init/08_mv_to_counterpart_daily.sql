-- Materialized View: stg_transactions_kafka → counterpart_daily_metrics
-- Legge dalla stessa sorgente Kafka della mv_to_daily.
-- Filtra NULL counterpart (POS/ATM) e operazioni di delete.

CREATE MATERIALIZED VIEW IF NOT EXISTS smash_olap.mv_to_counterpart_daily
            TO smash_olap.counterpart_daily_metrics AS
SELECT
    JSONExtractString(raw_json, 'after', 'customer_id')      AS customer_id,
    JSONExtractString(raw_json, 'after', 'counterpart')       AS counterpart_token,
    toDate(substring(
                   JSONExtractString(raw_json, 'after', 'transaction_date'), 1, 19
           ), 'Europe/Rome')                                         AS day,

    sumState(JSONExtractFloat(raw_json, 'after', 'amount'))       AS amt_sum_state,
    countState(JSONExtractFloat(raw_json, 'after', 'amount'))     AS amt_count_state,
    maxState(JSONExtractFloat(raw_json, 'after', 'amount'))       AS amt_max_state,
    minState(JSONExtractFloat(raw_json, 'after', 'amount'))       AS amt_min_state,
    avgState(JSONExtractFloat(raw_json, 'after', 'amount'))       AS amt_avg_state,
    stddevPopState(JSONExtractFloat(raw_json, 'after', 'amount')) AS amt_stddev_state,

    -- Categoria merchant dell'ultima transazione del giorno verso questa controparte
    argMaxState(
            JSONExtractString(raw_json, 'after', 'merchant_category'),
            toInt64(toUnixTimestamp(toDateTime(substring(
                    JSONExtractString(raw_json, 'after', 'transaction_date'), 1, 19
                                               ))))
    ) AS merchant_cat_state,

    -- Canale dell'ultima transazione del giorno verso questa controparte
    argMaxState(
            JSONExtractString(raw_json, 'after', 'channel'),
            toInt64(toUnixTimestamp(toDateTime(substring(
                    JSONExtractString(raw_json, 'after', 'transaction_date'), 1, 19
                                               ))))
    ) AS channel_state

FROM smash_olap.stg_transactions_kafka
WHERE
    JSONExtractString(raw_json, 'op') != 'd'
  AND JSONExtractString(raw_json, 'after', 'counterpart') != ''
  AND isNotNull(JSONExtractString(raw_json, 'after', 'counterpart'))
GROUP BY customer_id, counterpart_token, day;