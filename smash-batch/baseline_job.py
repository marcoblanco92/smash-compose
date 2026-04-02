"""
baseline_job.py — Calcola e pubblica le baseline OLAP su Kafka.

Flusso:
  1. Ogni 5 minuti legge da daily_metrics FINAL i clienti
     modificati nell'intervallo (delta processing)
  2. Per ogni cliente calcola w30/w90/w365 aggregati corretti
  3. Pubblica il record su topic customer.baselines
  4. Hazelcast viene aggiornato da CustomerBaselineUpdateFunction (Flink)

Aggregati corretti grazie a FINAL su query scheduled —
non su MV triggerata per ogni INSERT.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaProducer

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s'
)
logger = logging.getLogger('smash.baseline_job')

# ── Config da environment ─────────────────────────────────────
CLICKHOUSE_HOST     = os.getenv('CLICKHOUSE_HOST',     'clickhouse')
CLICKHOUSE_PORT     = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_DB       = os.getenv('CLICKHOUSE_DB',       'smash_olap')
CLICKHOUSE_USER     = os.getenv('CLICKHOUSE_USER',     'smash_olap_user')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'smash_olap_pwd')

KAFKA_BOOTSTRAP     = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC         = os.getenv('KAFKA_BASELINE_TOPIC',    'customer.baselines')

JOB_INTERVAL_MIN    = int(os.getenv('JOB_INTERVAL_MINUTES', '1'))
COLD_START_THRESHOLD = int(os.getenv('COLD_START_THRESHOLD', '5'))

# ── Query ─────────────────────────────────────────────────────
# Legge i clienti modificati nell'ultimo intervallo
# FINAL garantisce aggregati corretti su parti non ancora mergeate
QUERY_MODIFIED_CUSTOMERS = """
SELECT DISTINCT customer_id
FROM smash_olap.transactions_raw
WHERE ingested_at >= now() - INTERVAL {interval} MINUTE
"""

QUERY_BASELINE = """
WITH
    toDate(now('Europe/Rome'))                      AS today_it,
    toUInt32(countMerge(txn_count_state))           AS total_count_annual
SELECT
    customer_id,
    toUnixTimestamp64Milli(now64())                 AS computedAt,
    if(total_count_annual < {cold_start}, 1, 0)     AS isColdStart,

    -- w30
    round(sumMergeIf(txn_sum_state,    day >= today_it - 30), 2)          AS w30SumAmt,
    toUInt32(countMergeIf(txn_count_state, day >= today_it - 30))         AS w30Count,
    round(avgMergeIf(txn_avg_state,    day >= today_it - 30), 2)          AS w30AvgAmt,
    round(maxMergeIf(txn_max_state,    day >= today_it - 30), 2)          AS w30MaxAmt,
    round(stddevPopMergeIf(txn_stddev_state, day >= today_it - 30), 2)    AS w30StdDev,

    -- w30 weekly sums [w-3, w-2, w-1, w0]
    round(sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21), 2) AS w30Week3,
    round(sumMergeIf(txn_sum_state, day >= today_it - 21 AND day < today_it - 14), 2) AS w30Week2,
    round(sumMergeIf(txn_sum_state, day >= today_it - 14 AND day < today_it - 7),  2) AS w30Week1,
    round(sumMergeIf(txn_sum_state, day >= today_it - 7), 2)                          AS w30Week0,
    round(
        sumMergeIf(txn_sum_state, day >= today_it - 7) -
        sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21),
        2
    ) AS w30WeeklySlope,

    -- w90
    round(sumMergeIf(txn_sum_state,    day >= today_it - 90), 2)          AS w90SumAmt,
    toUInt32(countMergeIf(txn_count_state, day >= today_it - 90))         AS w90Count,
    round(avgMergeIf(txn_avg_state,    day >= today_it - 90), 2)          AS w90AvgAmt,
    round(maxMergeIf(txn_max_state,    day >= today_it - 90), 2)          AS w90MaxAmt,
    round(stddevPopMergeIf(txn_stddev_state, day >= today_it - 90), 2)    AS w90StdDev,

    -- w90 monthly sums [m-2, m-1, m0]
    round(sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60), 2) AS w90Month2,
    round(sumMergeIf(txn_sum_state, day >= today_it - 60 AND day < today_it - 30), 2) AS w90Month1,
    round(sumMergeIf(txn_sum_state, day >= today_it - 30), 2)                         AS w90Month0,
    round(
        sumMergeIf(txn_sum_state, day >= today_it - 30) -
        sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60),
        2
    ) AS w90MonthlySlope,

    -- w365
    round(sumMerge(txn_sum_state), 2)                                     AS w365SumAmt,
    total_count_annual                                                     AS w365Count,
    round(avgMerge(txn_avg_state), 2)                                     AS w365AvgAmt,
    round(maxMerge(txn_max_state), 2)                                     AS w365MaxAmt,
    round(stddevPopMerge(txn_stddev_state), 2)                            AS w365StdDev,

    -- Mappe qualitative 30d
    CAST(sumMapMergeIf(cat_amounts_state,   day >= today_it - 30), 'Map(String, Float64)') AS merchantCatAmounts30d,
    CAST(sumMapMergeIf(cat_counts_state,    day >= today_it - 30), 'Map(String, UInt32)')  AS merchantCatCounts30d,
    CAST(sumMapMergeIf(channel_counts_state,day >= today_it - 30), 'Map(String, UInt32)')  AS channelCounts30d,
    round(sumMergeIf(income_sum_state,      day >= today_it - 30), 2)                      AS estimatedMonthlyIncome,
    toUInt32(uniqMergeIf(counterparts_hll,  day >= today_it - 30))                         AS distinctCounterparts30d

FROM smash_olap.daily_metrics FINAL
WHERE customer_id = '{customer_id}'
GROUP BY customer_id
"""


def get_clickhouse_client() -> ClickHouseClient:
    return ClickHouseClient(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        settings={'use_numpy': False}
    )


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
        retries=3
    )


def build_baseline_record(row: dict) -> dict:
    """Trasforma la riga ClickHouse nel record Kafka atteso da CustomerBaseline.java"""
    return {
        'customerId':             row['customer_id'],
        'computedAt':             str(row['computedAt']),
        'isColdStart':            row['isColdStart'],
        'w30SumAmt':              row['w30SumAmt'],
        'w30Count':               row['w30Count'],
        'w30AvgAmt':              row['w30AvgAmt'],
        'w30MaxAmt':              row['w30MaxAmt'],
        'w30StdDev':              row['w30StdDev'],
        'w30WeeklySums':          [row['w30Week3'], row['w30Week2'], row['w30Week1'], row['w30Week0']],
        'w30WeeklySlope':         row['w30WeeklySlope'],
        'w90SumAmt':              row['w90SumAmt'],
        'w90Count':               row['w90Count'],
        'w90AvgAmt':              row['w90AvgAmt'],
        'w90MaxAmt':              row['w90MaxAmt'],
        'w90StdDev':              row['w90StdDev'],
        'w90MonthlySums':         [row['w90Month2'], row['w90Month1'], row['w90Month0']],
        'w90MonthlySlope':        row['w90MonthlySlope'],
        'w365SumAmt':             row['w365SumAmt'],
        'w365Count':              row['w365Count'],
        'w365AvgAmt':             row['w365AvgAmt'],
        'w365MaxAmt':             row['w365MaxAmt'],
        'w365StdDev':             row['w365StdDev'],
        'merchantCatAmounts30d':  dict(row['merchantCatAmounts30d']),
        'merchantCatCounts30d':   dict(row['merchantCatCounts30d']),
        'channelCounts30d':       dict(row['channelCounts30d']),
        'estimatedMonthlyIncome': row['estimatedMonthlyIncome'],
        'distinctCounterparts30d':row['distinctCounterparts30d'],
    }


def run_baseline_job():
    """Job principale — eseguito ogni JOB_INTERVAL_MIN minuti."""
    start = time.time()
    logger.info("── Baseline job avviato ──────────────────────────────")

    ch = None
    producer = None
    published = 0
    errors = 0

    try:
        ch = get_clickhouse_client()
        producer = get_kafka_producer()

        # 1. Clienti modificati nell'ultimo intervallo
        modified_query = QUERY_MODIFIED_CUSTOMERS.format(
            interval=JOB_INTERVAL_MIN
        )
        modified = ch.execute(modified_query)
        customer_ids = [row[0] for row in modified]

        logger.info("Clienti modificati: %d", len(customer_ids))

        if not customer_ids:
            logger.info("Nessun cliente modificato — skip.")
            return

        # 2. Per ogni cliente calcola e pubblica la baseline
        for customer_id in customer_ids:
            try:
                query = QUERY_BASELINE.format(
                    customer_id=customer_id,
                    cold_start=COLD_START_THRESHOLD
                )
                rows = ch.execute(query, with_column_types=True)
                data, columns = rows

                if not data:
                    continue

                col_names = [col[0] for col in columns]
                row_dict = dict(zip(col_names, data[0]))
                record = build_baseline_record(row_dict)

                producer.send(
                    KAFKA_TOPIC,
                    key=customer_id,
                    value=record
                )
                published += 1

            except Exception as e:
                logger.error("Errore cliente %s: %s", customer_id, e)
                errors += 1

        producer.flush()
        elapsed = round(time.time() - start, 2)
        logger.info(
            "── Job completato | pubblicati=%d errori=%d elapsed=%ss ──",
            published, errors, elapsed
        )

    except Exception as e:
        logger.error("Errore critico nel job: %s", e)
    finally:
        if producer:
            producer.close()


def main():
    logger.info("smash-batch avviato | intervallo=%d min | topic=%s",
                JOB_INTERVAL_MIN, KAFKA_TOPIC)

    # Esegui subito al boot per popolare la cache
    logger.info("Run iniziale al boot...")
    run_baseline_job()

    # Poi ogni N minuti
    scheduler = BlockingScheduler(timezone='Europe/Rome')
    scheduler.add_job(
        run_baseline_job,
        'interval',
        minutes=JOB_INTERVAL_MIN,
        id='baseline_job',
        max_instances=1,          # no overlap se il job è lento
        misfire_grace_time=60     # tollera 60s di ritardo prima di skippare
    )

    logger.info("Scheduler avviato. Prossima run tra %d minuti.", JOB_INTERVAL_MIN)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("smash-batch fermato.")


if __name__ == '__main__':
    main()