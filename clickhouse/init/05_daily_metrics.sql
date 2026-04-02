CREATE TABLE IF NOT EXISTS smash_olap.daily_metrics
(
    customer_id       String,
    day               Date,

    -- Metriche base
    txn_sum_state     AggregateFunction(sum,       Float64),
    txn_count_state   AggregateFunction(count,     Float64),
    txn_max_state     AggregateFunction(max,        Float64),
    txn_avg_state     AggregateFunction(avg,        Float64),
    txn_stddev_state  AggregateFunction(stddevPop,  Float64),

    -- Distribuzione Categorie
    cat_amounts_state AggregateFunction(sumMap, Array(String), Array(Float64)),
    cat_counts_state  AggregateFunction(sumMap, Array(String), Array(UInt64)),

    -- Distribuzione Canali (per channelCounts30d)
    channel_counts_state AggregateFunction(sumMap, Array(String), Array(UInt64)),

    -- Income INBOUND: wire/instant con amount > 0 (per estimatedMonthlyIncome)
    income_sum_state  AggregateFunction(sum, Float64),

    -- HLL per conteggio controparti distinte
    counterparts_hll  AggregateFunction(uniq, String)
    )
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(day)
    ORDER BY (customer_id, day)
    TTL day + INTERVAL 15 MONTH;