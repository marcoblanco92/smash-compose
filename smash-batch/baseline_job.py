"""
baseline_job.py — Calcola e pubblica le baseline OLAP su Kafka.

v4 — aggiunge CounterpartProfile: per ogni cliente con attività recente,
     calcola metriche per ogni controparte (avg, stdDev, isRecurring,
     isSubscription, expectedNextDate, direction) e le pubblica nel campo
     'counterparts' del record customer.baselines.

v3 — aggiunge w180: 6 bucket mensili + slope lineare (regressione OLS).
     La slope lineare è il coefficiente angolare della retta che meglio
     approssima i 6 punti mensili — robusta alle variazioni singole.
     Usata da CepEvaluator per rilevare trend di accumulo sostenuto (P-01, P-04).
"""

import json
import logging
import os
import time
from datetime import date, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s'
)
logger = logging.getLogger('smash.baseline_job')

CLICKHOUSE_HOST     = os.getenv('CLICKHOUSE_HOST',     'clickhouse')
CLICKHOUSE_PORT     = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_DB       = os.getenv('CLICKHOUSE_DB',       'smash_olap')
CLICKHOUSE_USER     = os.getenv('CLICKHOUSE_USER',     'smash_olap_user')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'smash_olap_pwd')

KAFKA_BOOTSTRAP      = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC          = os.getenv('KAFKA_BASELINE_TOPIC',    'customer.baselines')
JOB_INTERVAL_MIN     = int(os.getenv('JOB_INTERVAL_MINUTES', '5'))
COLD_START_THRESHOLD = int(os.getenv('COLD_START_THRESHOLD', '5'))

# ============================================================
# QUERY — Clienti modificati di recente
# ============================================================

QUERY_MODIFIED_CUSTOMERS = """
SELECT DISTINCT customer_id
FROM smash_olap.transactions_raw
WHERE ingested_at >= now() - INTERVAL {interval} MINUTE
"""

# ============================================================
# QUERY — Baseline aggregata per cliente (invariata da v3)
# ============================================================

QUERY_BASELINE = """
WITH
    toDate(now('Europe/Rome'))                      AS today_it,
    toUInt32(countMerge(txn_count_state))           AS total_count_annual
SELECT
    customer_id,
    toUnixTimestamp64Milli(now64())                 AS computedAt,
    if(total_count_annual < {cold_start}, 1, 0)     AS isColdStart,

    -- ── w30 ──────────────────────────────────────────────────
    round(sumMergeIf(txn_sum_state,      day >= today_it - 30), 2)        AS w30SumAmt,
    toUInt32(countMergeIf(txn_count_state, day >= today_it - 30))         AS w30Count,
    round(avgMergeIf(txn_avg_state,      day >= today_it - 30), 2)        AS w30AvgAmt,
    round(maxMergeIf(txn_max_state,      day >= today_it - 30), 2)        AS w30MaxAmt,
    round(minMergeIf(txn_min_state,      day >= today_it - 30), 2)        AS w30MinAmt,
    round(stddevPopMergeIf(txn_stddev_state, day >= today_it - 30), 2)    AS w30StdDev,

    -- w30 weekly sums [w-3, w-2, w-1, w0]
    round(sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21), 2) AS w30Week3,
    round(sumMergeIf(txn_sum_state, day >= today_it - 21 AND day < today_it - 14), 2) AS w30Week2,
    round(sumMergeIf(txn_sum_state, day >= today_it - 14 AND day < today_it - 7),  2) AS w30Week1,
    round(sumMergeIf(txn_sum_state, day >= today_it - 7), 2)                          AS w30Week0,
    round(
        sumMergeIf(txn_sum_state, day >= today_it - 7) -
        sumMergeIf(txn_sum_state, day >= today_it - 30 AND day < today_it - 21), 2
    ) AS w30WeeklySlope,

    -- ── w90 ──────────────────────────────────────────────────
    round(sumMergeIf(txn_sum_state,      day >= today_it - 90), 2)        AS w90SumAmt,
    toUInt32(countMergeIf(txn_count_state, day >= today_it - 90))         AS w90Count,
    round(avgMergeIf(txn_avg_state,      day >= today_it - 90), 2)        AS w90AvgAmt,
    round(maxMergeIf(txn_max_state,      day >= today_it - 90), 2)        AS w90MaxAmt,
    round(minMergeIf(txn_min_state,      day >= today_it - 90), 2)        AS w90MinAmt,
    round(stddevPopMergeIf(txn_stddev_state, day >= today_it - 90), 2)    AS w90StdDev,

    -- w90 monthly sums [m-2, m-1, m0]
    round(sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60), 2) AS w90Month2,
    round(sumMergeIf(txn_sum_state, day >= today_it - 60 AND day < today_it - 30), 2) AS w90Month1,
    round(sumMergeIf(txn_sum_state, day >= today_it - 30), 2)                         AS w90Month0,
    round(
        sumMergeIf(txn_sum_state, day >= today_it - 30) -
        sumMergeIf(txn_sum_state, day >= today_it - 90 AND day < today_it - 60), 2
    ) AS w90MonthlySlope,

    -- ── w180 — 6 bucket mensili ───────────────────────────────
    round(sumMergeIf(txn_sum_state, day >= today_it - 180 AND day < today_it - 150), 2) AS w180Month5,
    round(sumMergeIf(txn_sum_state, day >= today_it - 150 AND day < today_it - 120), 2) AS w180Month4,
    round(sumMergeIf(txn_sum_state, day >= today_it - 120 AND day < today_it - 90),  2) AS w180Month3,
    round(sumMergeIf(txn_sum_state, day >= today_it - 90  AND day < today_it - 60),  2) AS w180Month2,
    round(sumMergeIf(txn_sum_state, day >= today_it - 60  AND day < today_it - 30),  2) AS w180Month1,
    round(sumMergeIf(txn_sum_state, day >= today_it - 30),                            2) AS w180Month0,
    round(sumMergeIf(txn_sum_state, day >= today_it - 180), 2)                          AS w180SumAmt,
    toUInt32(countMergeIf(txn_count_state, day >= today_it - 180))                      AS w180Count,

    -- ── w365 ─────────────────────────────────────────────────
    round(sumMerge(txn_sum_state), 2)                                     AS w365SumAmt,
    total_count_annual                                                     AS w365Count,
    round(avgMerge(txn_avg_state), 2)                                     AS w365AvgAmt,
    round(maxMerge(txn_max_state), 2)                                     AS w365MaxAmt,
    round(minMerge(txn_min_state), 2)                                     AS w365MinAmt,
    round(stddevPopMerge(txn_stddev_state), 2)                            AS w365StdDev,

    -- ── Mappe qualitative ─────────────────────────────────────
    CAST(sumMapMergeIf(cat_amounts_state,    day >= today_it - 30), 'Map(String, Float64)') AS merchantCatAmounts30d,
    CAST(sumMapMergeIf(cat_counts_state,     day >= today_it - 30), 'Map(String, UInt32)')  AS merchantCatCounts30d,
    CAST(sumMapMergeIf(cat_amounts_state,    day >= today_it - 90), 'Map(String, Float64)') AS merchantCatAmounts90d,
    CAST(sumMapMergeIf(cat_counts_state,     day >= today_it - 90), 'Map(String, UInt32)')  AS merchantCatCounts90d,
    CAST(sumMapMergeIf(channel_counts_state, day >= today_it - 30), 'Map(String, UInt32)')  AS channelCounts30d,
    round(sumMergeIf(income_sum_state,       day >= today_it - 30), 2)                      AS estimatedMonthlyIncome,
    toUInt32(uniqMergeIf(counterparts_hll,   day >= today_it - 30))                         AS distinctCounterparts30d

FROM smash_olap.daily_metrics FINAL
WHERE customer_id = '{customer_id}'
GROUP BY customer_id
"""

# ============================================================
# QUERY — Metriche counterpart per cliente (NUOVO v4)
# Una riga per counterpart_token, finestra 12 mesi.
# Esclude controparti senza token (già filtrate dalla MV).
# ============================================================

QUERY_COUNTERPARTS = """
SELECT
    counterpart_token,
    round(sumMerge(amt_sum_state), 2)              AS sum12m,
    toUInt32(countMerge(amt_count_state))          AS count12m,   -- ← countMerge, non sumMerge
    round(avgMerge(amt_avg_state), 2)              AS avg12m,
    round(maxMerge(amt_max_state), 2)              AS max12m,
    round(minMerge(amt_min_state), 2)              AS min12m,
    round(stddevPopMerge(amt_stddev_state), 2)     AS stddev12m,
    argMaxMerge(merchant_cat_state)                AS merchant_cat,
    argMaxMerge(channel_state)                     AS channel,
    min(day)                                       AS first_seen_day,
    max(day)                                       AS last_seen_day,
    countDistinct(toYYYYMM(day))                   AS active_months
FROM smash_olap.counterpart_daily_metrics FINAL
WHERE customer_id = '{customer_id}'
  AND day >= today() - INTERVAL 12 MONTH
GROUP BY counterpart_token
HAVING count12m >= 1
"""

# Canali tipici di pagamenti ricorrenti (abbonamenti, domiciliazioni)
RECURRING_CHANNELS = {'sepa_dd', 'wire', 'instant'}


# ============================================================
# HELPERS — matematica e OLS
# ============================================================

def linear_slope(values: list[float]) -> float:
    """
    Regressione lineare OLS su una lista di valori equidistanti.
    Restituisce il coefficiente angolare (slope) della retta di best fit.
    Indice 0 = punto più vecchio, indice N-1 = punto più recente.

    Formula OLS semplificata per x = [0, 1, 2, ..., N-1]:
      slope = (N * sum(i*y_i) - sum(i) * sum(y_i)) / (N * sum(i^2) - sum(i)^2)
    """
    n = len(values)
    if n < 2:
        return 0.0
    sum_x  = n * (n - 1) / 2
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6
    sum_y  = sum(values)
    sum_xy = sum(i * v for i, v in enumerate(values))
    denom  = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 0.0
    return round((n * sum_xy - sum_x * sum_y) / denom, 2)


def date_to_epoch_millis(d: date) -> int:
    """Converte un oggetto date in epoch milliseconds (UTC midnight)."""
    if d is None:
        return 0
    import calendar
    return int(calendar.timegm(d.timetuple())) * 1000


# ============================================================
# CLIENT FACTORIES
# ============================================================

def get_clickhouse_client() -> ClickHouseClient:
    return ClickHouseClient(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB, user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD, settings={'use_numpy': False}
    )


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all', retries=3
    )


# ============================================================
# BUILD — record baseline cliente (invariato da v3)
# ============================================================

def build_baseline_record(row: dict) -> dict:
    """Trasforma la riga ClickHouse nel record Kafka atteso da CustomerBaseline.java"""

    w180_monthly = [
        row['w180Month5'], row['w180Month4'], row['w180Month3'],
        row['w180Month2'], row['w180Month1'], row['w180Month0'],
    ]
    w90_monthly = [row['w90Month2'], row['w90Month1'], row['w90Month0']]

    return {
        'customerId':   row['customer_id'],
        'computedAt':   str(row['computedAt']),
        'isColdStart':  row['isColdStart'],

        # w30
        'w30SumAmt':      row['w30SumAmt'],
        'w30Count':       row['w30Count'],
        'w30AvgAmt':      row['w30AvgAmt'],
        'w30MaxAmt':      row['w30MaxAmt'],
        'w30MinAmt':      row['w30MinAmt'],
        'w30StdDev':      row['w30StdDev'],
        'w30WeeklySums':  [row['w30Week3'], row['w30Week2'], row['w30Week1'], row['w30Week0']],
        'w30WeeklySlope': row['w30WeeklySlope'],

        # w90
        'w90SumAmt':       row['w90SumAmt'],
        'w90Count':        row['w90Count'],
        'w90AvgAmt':       row['w90AvgAmt'],
        'w90MaxAmt':       row['w90MaxAmt'],
        'w90MinAmt':       row['w90MinAmt'],
        'w90StdDev':       row['w90StdDev'],
        'w90MonthlySums':  w90_monthly,
        'w90MonthlySlope': linear_slope(w90_monthly),

        # w180
        'w180SumAmt':       row['w180SumAmt'],
        'w180Count':        row['w180Count'],
        'w180MonthlySums':  w180_monthly,
        'w180MonthlySlope': linear_slope(w180_monthly),

        # w365
        'w365SumAmt': row['w365SumAmt'],
        'w365Count':  row['w365Count'],
        'w365AvgAmt': row['w365AvgAmt'],
        'w365MaxAmt': row['w365MaxAmt'],
        'w365MinAmt': row['w365MinAmt'],
        'w365StdDev': row['w365StdDev'],

        # Mappe qualitative
        'merchantCatAmounts30d': dict(row['merchantCatAmounts30d']),
        'merchantCatCounts30d':  dict(row['merchantCatCounts30d']),
        'merchantCatAvgAmounts90d': {
            cat: round(amt / row['merchantCatCounts90d'].get(cat, 1), 2)
            for cat, amt in row['merchantCatAmounts90d'].items()
            if row['merchantCatCounts90d'].get(cat, 0) > 0
        },
        'channelCounts30d':        dict(row['channelCounts30d']),
        'estimatedMonthlyIncome':  row['estimatedMonthlyIncome'],
        'distinctCounterparts30d': row['distinctCounterparts30d'],

        # Counterparts — popolato da build_counterpart_profiles() nel job loop
        'counterparts': {},
    }


# ============================================================
# BUILD — profili counterpart (NUOVO v4)
# ============================================================

def build_counterpart_profiles(rows: list, col_names: list) -> dict:
    """
    Calcola CounterpartProfile per ogni controparte di un cliente.

    Logica detection:
      isRecurring    → count >= 3 AND active_months >= 3 AND avg_interval <= 45gg
      isSubscription → isRecurring AND cv_amount < 0.15 AND channel ricorrente

    Restituisce dict: counterpart_token → profile dict
    (compatibile con CounterpartProfile.java)
    """
    profiles = {}

    for row_tuple in rows:
        r = dict(zip(col_names, row_tuple))
        token = r.get('counterpart_token')
        if not token:
            continue

        count         = r.get('count12m') or 0
        avg_amt       = r.get('avg12m') or 0.0
        stddev_amt    = r.get('stddev12m') or 0.0
        sum12m        = r.get('sum12m') or 0.0
        channel       = r.get('channel') or ''
        active_months = r.get('active_months') or 0
        last_seen     = r.get('last_seen_day')   # oggetto date da ClickHouse
        first_seen    = r.get('first_seen_day')  # oggetto date da ClickHouse

        # ── Direction ──────────────────────────────────────────
        # INBOUND = accrediti (avg positivo), OUTBOUND = addebiti (avg negativo)
        direction = 'INBOUND' if avg_amt > 0 else 'OUTBOUND'

        # ── CV importo: stdDev / |avg| ─────────────────────────
        # Misura quanto è stabile l'importo: 0 = sempre uguale, 1 = molto variabile
        cv_amount = (stddev_amt / abs(avg_amt)) if abs(avg_amt) > 0.01 else 999.0

        # ── avgIntervalDays: stima da bucket mensili ───────────
        # Formula: (30 * mesi_attivi) / numero_pagamenti
        # Approssimazione sufficiente per POC — alternativa: calcolo esatto su date
        avg_interval_days = 0.0
        if active_months >= 2 and count >= 2:
            avg_interval_days = round((30.0 * active_months) / count, 1)

        # ── isRecurring ────────────────────────────────────────
        # Condizioni: almeno 3 pagamenti in almeno 3 mesi distinti,
        # con intervallo medio non superiore a 45 giorni
        is_recurring = (
            count >= 3
            and active_months >= 3
            and 0 < avg_interval_days <= 45
        )

        # ── isSubscription ─────────────────────────────────────
        # Sottocaso di isRecurring: importo quasi fisso + canale tipico abbonamento
        is_subscription = (
            is_recurring
            and cv_amount < 0.15
            and channel in RECURRING_CHANNELS
        )

        # ── expectedNextDate ───────────────────────────────────
        # Stima: ultimo pagamento + avgIntervalDays
        expected_next_date = 0
        if avg_interval_days > 0 and last_seen:
            delta = timedelta(days=round(avg_interval_days))
            expected_next_date = date_to_epoch_millis(last_seen + delta)

        # ── lastAmount: ultimo importo rilevante ───────────────
        # Per INBOUND prendiamo il massimo (accredito più recente di solito è il più alto)
        # Per OUTBOUND prendiamo il minimo (in valore assoluto = spesa più alta)
        last_amount = r.get('max12m', 0.0) if direction == 'INBOUND' else r.get('min12m', 0.0)

        profiles[token] = {
            'counterpartToken': token,
            'direction':        direction,

            # Storico importi
            'paymentCount12m': count,
            'sumAmount12m':    sum12m,
            'avgAmount12m':    avg_amt,
            'stdDev12m':       stddev_amt,
            'lastAmount':      last_amount,
            'minAmount12m':    r.get('min12m', 0.0),
            'maxAmount12m':    r.get('max12m', 0.0),

            # Frequenza e timing
            'avgIntervalDays':  avg_interval_days,
            'isRecurring':      is_recurring,
            'isSubscription':   is_subscription,
            'expectedNextDate': expected_next_date,
            'lastDate':         date_to_epoch_millis(last_seen),
            'firstSeenDate':    date_to_epoch_millis(first_seen),
            'monthsActive':     active_months,

            # Merchant info
            'merchantCategory': r.get('merchant_cat') or '',
            'channel':          channel,

            # Metadata
            'lastUpdateTs': int(time.time() * 1000),
        }

    return profiles


# ============================================================
# JOB PRINCIPALE
# ============================================================

def run_baseline_job():
    start = time.time()
    logger.info("── Baseline job avviato ──────────────────────────────")

    ch = producer = None
    published = errors = 0

    try:
        ch = get_clickhouse_client()
        producer = get_kafka_producer()

        # Step 1: clienti con transazioni recenti
        modified = ch.execute(
            QUERY_MODIFIED_CUSTOMERS.format(interval=JOB_INTERVAL_MIN)
        )
        customer_ids = [row[0] for row in modified]
        logger.info("Clienti modificati: %d", len(customer_ids))

        if not customer_ids:
            logger.info("Nessun cliente modificato — skip.")
            return

        for customer_id in customer_ids:
            try:
                # Step 2: baseline aggregata (w30/w90/w180/w365 + mappe)
                rows = ch.execute(
                    QUERY_BASELINE.format(
                        customer_id=customer_id,
                        cold_start=COLD_START_THRESHOLD
                    ),
                    with_column_types=True
                )
                data, columns = rows
                if not data:
                    continue

                row_dict = dict(zip([c[0] for c in columns], data[0]))
                record   = build_baseline_record(row_dict)

                # Step 3: metriche counterpart (NUOVO v4)
                # Safe default: se fallisce non blocca la pubblicazione del baseline
                try:
                    cp_result = ch.execute(
                        QUERY_COUNTERPARTS.format(customer_id=customer_id),
                        with_column_types=True
                    )
                    cp_data, cp_columns = cp_result
                    cp_col_names = [c[0] for c in cp_columns]
                    record['counterparts'] = build_counterpart_profiles(cp_data, cp_col_names)
                    logger.debug(
                        "Controparti calcolate per %s: %d",
                        customer_id, len(record['counterparts'])
                    )
                except Exception as cp_err:
                    logger.warning(
                        "Errore counterpart per %s: %s — pubblicato senza counterparts",
                        customer_id, cp_err
                    )
                    record['counterparts'] = {}

                # Step 4: pubblica su Kafka
                producer.send(KAFKA_TOPIC, key=customer_id, value=record)
                published += 1

            except Exception as e:
                logger.error("Errore cliente %s: %s", customer_id, e)
                errors += 1

        producer.flush()
        logger.info(
            "── Job completato | pubblicati=%d errori=%d elapsed=%ss ──",
            published, errors, round(time.time() - start, 2)
        )

    except Exception as e:
        logger.error("Errore critico nel job: %s", e)
    finally:
        if producer:
            producer.close()


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    logger.info("smash-batch avviato | intervallo=%d min | topic=%s",
                JOB_INTERVAL_MIN, KAFKA_TOPIC)
    logger.info("Run iniziale al boot...")
    run_baseline_job()

    scheduler = BlockingScheduler(timezone='Europe/Rome')
    scheduler.add_job(
        run_baseline_job, 'interval', minutes=JOB_INTERVAL_MIN,
        id='baseline_job', max_instances=1, misfire_grace_time=60
    )
    logger.info("Scheduler avviato. Prossima run tra %d minuti.", JOB_INTERVAL_MIN)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("smash-batch fermato.")


if __name__ == '__main__':
    main()