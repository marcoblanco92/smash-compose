-- ============================================================
-- 05_cep_wire_anomalo.sql
-- Pattern: P-05 — Wire Anomalo
--
-- UUID fissi:
--   CUSTOMER   : 10000005-0000-0000-0000-000000000001
--   ACCOUNT    : 20000005-0000-0000-0000-000000000001
--   LOAN       : 40000005-0000-0000-0000-000000000001
--   CARD DEBIT : 50000005-0000-0000-0000-000000000001
--   CRM        : 30000005-0000-0000-0000-000000000001
--
-- Logica:
--   wireCount30d >= 3                      → storico sufficiente
--   wireFromAppPct30d < 0.80               → cliente digitale ma wire non da app
--   (txnTs - lastAppEventTs) > 30s         → nessuna sessione app recente
--   → P-05
--
-- ⚠️  ESECUZIONE IN 4 STEP SEPARATI — non eseguire tutto insieme.
--     L'ordine garantisce che i wire storici vengano processati da Flink
--     PRIMA che lastAppEventTs venga aggiornato dall'app event recente.
--     Se i wire arrivano dopo l'app event, Flink li classifica come
--     "da app" → wireFromAppPct30d sale → P-05 non scatta.
--
-- STEP 1: anagrafica + cold start → esegui, poi aspetta 5 min (smash-batch)
-- STEP 2: wire storici → esegui, poi aspetta 15s (Flink processing)
-- STEP 3: app event recente → esegui, poi aspetta 5s
-- STEP 4: wire trigger → esegui → verifica events.enriched
--
-- Output atteso: detectedPatterns: ["P-05"]
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- STEP 1 — Anagrafica + cold start
-- Esegui questo blocco, poi attendi ~5 minuti che smash-batch
-- pubblichi la baseline e superi il COLD_START_THRESHOLD=5.
-- ============================================================

INSERT INTO smash_own.customers (customer_id, first_name, last_name, birth_date, birth_place, tax_code,
                                 segment, pattern_type, pattern_trigger_date, active_pattern,
                                 risk_class, clv_score, relationship_mgr, onboarding_date, is_active)
VALUES ('10000005-0000-0000-0000-000000000001'::uuid,
        'Carlo', 'Esposito', '1978-09-03'::date, 'Napoli', 'SPTCRL78P03F839Z',
        'retail', 'ordinary', NULL, 'ordinary',
        'low', 62.00,
        '99000005-0000-0000-0000-000000000001'::uuid,
        CURRENT_DATE - INTERVAL '2 years', TRUE);

INSERT INTO smash_own.accounts (account_id, customer_id, account_type, iban, currency,
                                current_balance, opened_date, status, overdraft_limit, updated_at)
VALUES ('20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        'checking', 'IT60X0542811101000000123406', 'EUR',
        5800.00, CURRENT_DATE - INTERVAL '2 years', 'active', 0.00, NOW());

INSERT INTO smash_own.cards (card_id, customer_id, account_id, card_type, card_number,
                             plafond_limit, plafond_used, billing_cycle_day,
                             status, issued_date, expiry_date, updated_at)
VALUES ('50000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        '20000005-0000-0000-0000-000000000001'::uuid,
        'debit', '4555555555550001',
        NULL, 0.00, NULL,
        'active', CURRENT_DATE - INTERVAL '2 years',
        CURRENT_DATE + INTERVAL '2 years', NOW());

INSERT INTO smash_own.loans (loan_id, customer_id, loan_type, principal_amount,
                             outstanding_balance, interest_rate, start_date,
                             maturity_date, next_due_date, days_past_due,
                             credit_line_usage_pct, avg_payment_delay_days,
                             status, collateral_type, updated_at)
VALUES ('40000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        'personal', 6000.00, 2400.00, 6.800,
        CURRENT_DATE - INTERVAL '1 year',
        CURRENT_DATE + INTERVAL '1 year',
        CURRENT_DATE + INTERVAL '18 days',
        0, NULL, 0,
        'active', 'none', NOW());

INSERT INTO smash_own.crm_profiles (profile_id, customer_id, segment, products_held,
                                    has_mortgage, has_investments, clv_score, churn_risk_score,
                                    relationship_mgr, last_contact_date, preferred_channel,
                                    push_opt_in, avg_session_duration_30d, push_ignore_streak,
                                    days_since_last_contact, product_usage_score, updated_at)
VALUES ('30000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        'retail', '[
    "checking"
  ]'::jsonb,
        FALSE, FALSE, 62.00, 0.120,
        '99000005-0000-0000-0000-000000000001'::uuid,
        CURRENT_DATE - INTERVAL '15 days',
        'app', TRUE, 250, 0, 15, 0.550, NOW());

INSERT INTO smash_own.market_data (record_id, data_type, metric_name, value, previous_value,
                                   recorded_at, source)
VALUES (gen_random_uuid(), 'ecb_rate', 'ecb_deposit_rate',
        3.25000, 3.25000, NOW() - INTERVAL '1 day', 'synthetic');

-- App events storici — non disturbano lastAppEventTs corrente
INSERT INTO smash_own.app_events (event_id, customer_id, event_type, screen_name,
                                  session_id, session_duration_s, event_timestamp,
                                  device_type, is_push_opened, feature_category,
                                  screens_visited_n, is_return_visit)
VALUES (gen_random_uuid(),
        '10000005-0000-0000-0000-000000000001'::uuid,
        'screen_view', 'bonifici/nuovo',
        gen_random_uuid(), 180, NOW() - INTERVAL '21 days',
        'ios', FALSE, 'essential', 3, FALSE),

       (gen_random_uuid(),
        '10000005-0000-0000-0000-000000000001'::uuid,
        'screen_view', 'dashboard/home',
        gen_random_uuid(), 145, NOW() - INTERVAL '14 days',
        'ios', FALSE, 'essential', 2, FALSE),

       (gen_random_uuid(),
        '10000005-0000-0000-0000-000000000001'::uuid,
        'screen_view', 'movimenti/lista',
        gen_random_uuid(), 210, NOW() - INTERVAL '7 days',
        'ios', FALSE, 'essential', 3, FALSE);

-- Transazioni cold start (NON wire — non disturbano wireCount30d)
-- Fuori dalla finestra 30gg → contano solo per w365Count in ClickHouse
-- Portano w365Count a 6 → sopra COLD_START_THRESHOLD=5
INSERT INTO smash_own.transactions (transaction_id, account_id, customer_id, amount, currency,
                                    merchant_category, channel, counterpart, card_id,
                                    transaction_date, value_date, description, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        3000.00, 'EUR', 'salary_income', 'sepa_dd',
        'IT00EMPLOYER0000000001', NULL,
        NOW() - INTERVAL '60 days', (NOW() - INTERVAL '60 days')::date,
        NULL, TRUE, 'ordinary_baseline'),

       (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -850.00, 'EUR', 'utilities', 'sepa_dd',
        'IT00UTILITY000000000001', NULL,
        NOW() - INTERVAL '58 days', (NOW() - INTERVAL '58 days')::date,
        NULL, TRUE, 'ordinary_baseline'),

       (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -400.00, 'EUR', 'grocery', 'pos',
        NULL, '50000005-0000-0000-0000-000000000001'::uuid,
        NOW() - INTERVAL '55 days', (NOW() - INTERVAL '55 days')::date,
        NULL, FALSE, 'ordinary_baseline'),

-- Trigger smash-batch — pos NOW() → ingested_at recente → smash-batch vede il cliente
       (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -5.00, 'EUR', 'grocery', 'pos', NULL,
        '50000005-0000-0000-0000-000000000001'::uuid,
        NOW(), NOW()::date, NULL, FALSE, 'ordinary_baseline');

-- ⏱️  PAUSA ~5 MINUTI
-- Verifica: Kafka UI → customer.baselines → isColdStart=0
-- channelCounts30d.wire dovrebbe essere 0 (nessun wire ancora)
-- Poi esegui STEP 2.


-- ============================================================
-- STEP 2 — Wire storici
-- Esegui SOLO dopo aver ricevuto la baseline con isColdStart=0.
-- Flink processa questi wire con lastAppEventTs = NOW()-7gg
-- → gap = 7gg >> 30s → NON contati come "da app"
-- → wireFromAppPct30d resta 0%
-- Dopo l'esecuzione aspetta ~15 secondi prima di STEP 3.
-- ============================================================

INSERT INTO smash_own.transactions (transaction_id, account_id, customer_id, amount, currency,
                                    merchant_category, channel, counterpart, card_id,
                                    transaction_date, value_date, description, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -1800.00, 'EUR', 'internal_transfer', 'wire',
        'IT00EXTERNAL0IBAN0AAA01', NULL,
        NOW() - INTERVAL '25 days', (NOW() - INTERVAL '25 days')::date,
        NULL, FALSE, 'ordinary_baseline'),

       (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -2200.00, 'EUR', 'internal_transfer', 'wire',
        'IT00EXTERNAL0IBAN0BBB02', NULL,
        NOW() - INTERVAL '18 days', (NOW() - INTERVAL '18 days')::date,
        NULL, FALSE, 'ordinary_baseline'),

       (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -950.00, 'EUR', 'internal_transfer', 'wire',
        'IT00EXTERNAL0IBAN0CCC03', NULL,
        NOW() - INTERVAL '10 days', (NOW() - INTERVAL '10 days')::date,
        NULL, FALSE, 'ordinary_baseline');

-- ⏱️  PAUSA ~15 SECONDI
-- Verifica: Kafka UI → events.enriched → wireCount30d = 3, wireFromAppPct30d = 0.0
-- Poi esegui STEP 3.


-- ============================================================
-- STEP 3 — App event recente
-- Aggiorna lastAppEventTs a NOW()-10min nel profilo Flink.
-- Simula il cliente che apre l'app ma NON dispone il bonifico
-- dall'app — il bonifico arriverà da canale esterno (STEP 4).
-- Dopo l'esecuzione aspetta ~5 secondi prima di STEP 4.
-- ============================================================

INSERT INTO smash_own.app_events (event_id, customer_id, event_type, screen_name,
                                  session_id, session_duration_s, event_timestamp,
                                  device_type, is_push_opened, feature_category,
                                  screens_visited_n, is_return_visit)
VALUES (gen_random_uuid(),
        '10000005-0000-0000-0000-000000000001'::uuid,
        'screen_view', 'dashboard/home',
        gen_random_uuid(), 65,
        NOW() - INTERVAL '10 minutes',
        'ios', FALSE, 'essential', 1, FALSE);

-- ⏱️  PAUSA ~5 SECONDI — poi esegui STEP 4.


-- ============================================================
-- STEP 4 — Wire trigger
-- Condizioni al momento dell'esecuzione:
--   wireCount30d    = 3 (da OLAP baseline) + 1 (questo) = 4 >= 3 ✓
--   wireFromAppPct30d = 0/3 = 0% < 80% ✓
--   lastAppEventTs  = NOW() - 10min → gap = 600s >> 30s ✓
-- → P-05 scatta
-- ============================================================

INSERT INTO smash_own.transactions (transaction_id, account_id, customer_id, amount, currency,
                                    merchant_category, channel, counterpart, card_id,
                                    transaction_date, value_date, description, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        '20000005-0000-0000-0000-000000000001'::uuid,
        '10000005-0000-0000-0000-000000000001'::uuid,
        -3500.00, 'EUR', 'internal_transfer', 'wire',
        'IT00EXTERNAL0IBAN0DDD04', NULL,
        NOW(), CURRENT_DATE, NULL, FALSE, 'ordinary_baseline');

-- ── VERIFICA ──────────────────────────────────────────────
SELECT channel::text,
       COUNT(*)              AS wire_count,
       MIN(transaction_date) AS prima,
       MAX(transaction_date) AS ultima
FROM smash_own.transactions
WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid
  AND channel IN ('wire', 'instant')
  AND transaction_date >= NOW() - INTERVAL '30 days'
GROUP BY channel;

SELECT screen_name, event_timestamp
FROM smash_own.app_events
WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid
ORDER BY event_timestamp DESC
LIMIT 5;

SELECT EXTRACT(EPOCH FROM (
    (SELECT MAX(transaction_date)
     FROM smash_own.transactions
     WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid
       AND channel = 'wire')
        -
    (SELECT MAX(event_timestamp)
     FROM smash_own.app_events
     WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid)
    ))::int    AS gap_secondi,
       CASE
           WHEN EXTRACT(EPOCH FROM (
               (SELECT MAX(transaction_date)
                FROM smash_own.transactions
                WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid
                  AND channel = 'wire')
                   -
               (SELECT MAX(event_timestamp)
                FROM smash_own.app_events
                WHERE customer_id = '10000005-0000-0000-0000-000000000001'::uuid)
               )) > 30
               THEN '✓ gap > 30s → P-05 atteso'
           ELSE '✗ gap <= 30s → P-05 NON scatterà'
           END AS valutazione;