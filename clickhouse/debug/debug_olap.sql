-- ============================================================
-- debug_olap.sql — Query di debug per daily_metrics e baselines
-- Esegui sulla Play UI ClickHouse http://localhost:8123/play
-- ============================================================

-- ── 1. Quante righe in daily_metrics? ────────────────────────
SELECT count()           AS total_rows,
       uniq(customer_id) AS clienti_distinti,
       min(day)          AS giorno_piu_vecchio,
       max(day)          AS giorno_piu_recente
FROM smash_olap.daily_metrics;

-- ── 2. Aggregati per giorno — vista leggibile ────────────────
-- Demerge degli stati aggregati per leggere i valori reali
SELECT customer_id,
       day,
       round(sumMerge(txn_sum_state), 2)                            AS txn_sum,
       toUInt32(countMerge(txn_count_state))                        AS txn_count,
       round(avgMerge(txn_avg_state), 2)                            AS txn_avg,
       round(maxMerge(txn_max_state), 2)                            AS txn_max,
       round(stddevPopMerge(txn_stddev_state), 2)                   AS txn_stddev,
       toUInt32(uniqMerge(counterparts_hll))                        AS distinct_counterparts,
       CAST(sumMapMerge(cat_amounts_state), 'Map(String, Float64)') AS cat_amounts,
       CAST(sumMapMerge(cat_counts_state), 'Map(String, UInt32)')   AS cat_counts
FROM smash_olap.daily_metrics
GROUP BY customer_id, day
ORDER BY customer_id, day DESC
LIMIT 50;

-- ── 3. Baseline per cliente — simula mv_push_to_kafka ────────
-- Sostituisci 'CUSTOMER_ID' con il valore da testare
WITH
    toDate(now('Europe/Rome')) AS today_it, toUInt32(countMerge(txn_count_state)) AS total_count_annual
SELECT customer_id,

       -- Cold start
       if(total_count_annual < 5, 1, 0)                                                     AS isColdStart,

       -- w30
       round(sumMergeIf(txn_sum_state, day >= today_it - 30), 2)                            AS w30_sum,
       toUInt32(countMergeIf(txn_count_state, day >= today_it - 30))                        AS w30_count,
       round(avgMergeIf(txn_avg_state, day >= today_it - 30), 2)                            AS w30_avg,

       -- w30 weekly sums [w-3, w-2, w-1, w0]
       round(sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21), 2)    AS w30_week_minus3,
       round(sumMergeIf(txn_sum_state, day >= today_it - 21 AND day < today_it - 14), 2)    AS w30_week_minus2,
       round(sumMergeIf(txn_sum_state, day >= today_it - 14 AND day < today_it - 7), 2)     AS w30_week_minus1,
       round(sumMergeIf(txn_sum_state, day >= today_it - 7), 2)                             AS w30_week0,
       round(
               sumMergeIf(txn_sum_state, day >= today_it - 7) -
               sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21),
               2
       )                                                                                    AS w30_weekly_slope,

       -- w90
       round(sumMergeIf(txn_sum_state, day >= today_it - 90), 2)                            AS w90_sum,
       toUInt32(countMergeIf(txn_count_state, day >= today_it - 90))                        AS w90_count,

       -- w90 monthly sums [m-2, m-1, m0]
       round(sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60), 2)    AS w90_month_minus2,
       round(sumMergeIf(txn_sum_state, day >= today_it - 60 AND day < today_it - 30), 2)    AS w90_month_minus1,
       round(sumMergeIf(txn_sum_state, day >= today_it - 30), 2)                            AS w90_month0,
       round(
               sumMergeIf(txn_sum_state, day >= today_it - 30) -
               sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60),
               2
       )                                                                                    AS w90_monthly_slope,

       -- w365
       round(sumMerge(txn_sum_state), 2)                                                    AS w365_sum,
       total_count_annual                                                                   AS w365_count,

       -- mappe
       CAST(sumMapMergeIf(cat_amounts_state, day >= today_it - 30), 'Map(String, Float64)') AS cat_amounts_30d,
       toUInt32(uniqMergeIf(counterparts_hll, day >= today_it - 30))                        AS distinct_counterparts_30d

FROM smash_olap.daily_metrics
-- WHERE customer_id = 'CUSTOMER_ID'   -- decommenta per filtrare su un cliente
GROUP BY customer_id
ORDER BY customer_id
LIMIT 20;

-- ── 4. Verifica copertura giorni per cliente ──────────────────
-- Utile per capire se ci sono buchi nel dato storico
SELECT customer_id,
       count()                                             AS giorni_presenti,
       min(day)                                            AS dal,
       max(day)                                            AS al,
       dateDiff('day', min(day), max(day)) + 1             AS giorni_attesi,
       count() / (dateDiff('day', min(day), max(day)) + 1) AS copertura_pct
FROM smash_olap.daily_metrics
GROUP BY customer_id
ORDER BY copertura_pct ASC
LIMIT 20;

-- ── 5. Top clienti per spesa w30 ─────────────────────────────
WITH toDate(now('Europe/Rome')) AS today_it
SELECT customer_id,
       round(sumMergeIf(txn_sum_state, day >= today_it - 30), 2)     AS w30_sum,
       toUInt32(countMergeIf(txn_count_state, day >= today_it - 30)) AS w30_count
FROM smash_olap.daily_metrics
GROUP BY customer_id
ORDER BY w30_sum DESC
LIMIT 10;

-- ── 6. Verifica mv_push_to_kafka — output diretto ────────────
-- Legge dalla vista della MV (senza consumare il topic Kafka)
SELECT customerId,
       computedAt,
       isColdStart,
       w30SumAmt,
       w30Count,
       w30WeeklySums,
       w30WeeklySlope,
       w90SumAmt,
       w90Count,
       w90MonthlySums,
       w90MonthlySlope,
       w365SumAmt,
       w365Count,
       distinctCounterparts30d
FROM smash_olap.mv_push_to_kafka
LIMIT 10;