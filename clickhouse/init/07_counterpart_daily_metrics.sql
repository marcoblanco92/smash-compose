-- counterpart_daily_metrics
-- Aggregati giornalieri per coppia (customer_id, counterpart_token).
-- Alimentata da mv_to_counterpart_daily (vedi file 08).
-- TTL 15 mesi — stesso orizzonte di daily_metrics.
-- Esclude transazioni senza controparte (POS, ATM).

CREATE TABLE IF NOT EXISTS smash_olap.counterpart_daily_metrics
(
    customer_id         String,
    counterpart_token   String,                          -- mai null: filtrato dalla MV
    day                 Date,

    -- Raw amounts (con segno: + INBOUND, - OUTBOUND)
    amt_sum_state       AggregateFunction(sum,      Float64),
    amt_count_state     AggregateFunction(count,    Float64),
    amt_max_state       AggregateFunction(max,      Float64),
    amt_min_state       AggregateFunction(min,      Float64),
    amt_avg_state       AggregateFunction(avg,      Float64),
    amt_stddev_state    AggregateFunction(stddevPop, Float64),

    -- Categoria merchant più frequente (per isSubscription detection)
    merchant_cat_state  AggregateFunction(argMax, String, Int64),

    -- Canale più frequente (wire/sepa_dd → ricorrente, pos → spot)
    channel_state       AggregateFunction(argMax, String, Int64)

    )
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(day)
    ORDER BY (customer_id, counterpart_token, day)
    TTL day + INTERVAL 15 MONTH;