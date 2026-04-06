-- ============================================================
-- 03_cep_pre_churn.sql
-- Pattern: P-03-PHASE1 — Pre-Churn (Distanza Emotiva)
--
-- UUID fissi:
--   CUSTOMER  : 10000003-0000-0000-0000-000000000001
--   ACCOUNT   : 20000003-0000-0000-0000-000000000001
--   LOAN      : 40000003-0000-0000-0000-000000000001
--   CARD DEBIT: 50000003-0000-0000-0000-000000000001
--   CRM       : 30000003-0000-0000-0000-000000000001
--
-- Condizioni (2 di 3):
--   ① hasFeatureNarrowing = true   (solo 'essential' ultimi 14gg)
--   ② pushIgnoreStreak >= 3        (= 4)
--   ③ sessionDrop > 30%            (175s vs baseline 320s → 45%)
--
-- ⚠️  DIPENDENZA smash-batch: 4 transazioni ordinarie (entro 14gg) superano
--     COLD_START_THRESHOLD=5. Attendere ~5 min dopo l'esecuzione prima del trigger.
-- Output atteso: detectedPatterns: ["P-03-PHASE1"]
-- ============================================================

SET search_path TO smash_own;

INSERT INTO smash_own.customers (
    customer_id, first_name, last_name, birth_date, birth_place, tax_code,
    segment, pattern_type, pattern_trigger_date, active_pattern,
    risk_class, clv_score, relationship_mgr, onboarding_date, is_active
) VALUES (
             '10000003-0000-0000-0000-000000000001'::uuid,
             'Luca', 'Ferrari', '1990-07-25'::date, 'Bologna', 'FRRLCU90L25A944G',
             'retail', 'pre_churn', CURRENT_DATE, 'pre_churn',
             'medium', 58.00,
             '99000003-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '3 years', TRUE
         );

INSERT INTO smash_own.accounts (
    account_id, customer_id, account_type, iban, currency,
    current_balance, opened_date, status, overdraft_limit, updated_at
) VALUES (
             '20000003-0000-0000-0000-000000000001'::uuid,
             '10000003-0000-0000-0000-000000000001'::uuid,
             'checking', 'IT60X0542811101000000123404', 'EUR',
             3200.00, CURRENT_DATE - INTERVAL '3 years', 'active', 0.00, NOW()
         );

INSERT INTO smash_own.cards (
    card_id, customer_id, account_id, card_type, card_number,
    plafond_limit, plafond_used, billing_cycle_day,
    status, issued_date, expiry_date, updated_at
) VALUES (
             '50000003-0000-0000-0000-000000000001'::uuid,
             '10000003-0000-0000-0000-000000000001'::uuid,
             '20000003-0000-0000-0000-000000000001'::uuid,
             'debit', '4333333333330001',
             NULL, 0.00, NULL,
             'active', CURRENT_DATE - INTERVAL '3 years',
             CURRENT_DATE + INTERVAL '1 year', NOW()
         );

-- Prestito personale piccolo — nessun segnale di stress
INSERT INTO smash_own.loans (
    loan_id, customer_id, loan_type, principal_amount,
    outstanding_balance, interest_rate, start_date,
    maturity_date, next_due_date, days_past_due,
    credit_line_usage_pct, avg_payment_delay_days,
    status, collateral_type, updated_at
) VALUES (
             '40000003-0000-0000-0000-000000000001'::uuid,
             '10000003-0000-0000-0000-000000000001'::uuid,
             'personal', 5000.00, 1800.00, 7.200,
             CURRENT_DATE - INTERVAL '18 months',
             CURRENT_DATE + INTERVAL '6 months',
             CURRENT_DATE + INTERVAL '20 days',
             0, NULL, 0,
             'active', 'none', NOW()
         );

-- push_ignore_streak=4 → ②   avg_session_duration_30d=320 → baseline ③
INSERT INTO smash_own.crm_profiles (
    profile_id, customer_id, segment, products_held,
    has_mortgage, has_investments, clv_score, churn_risk_score,
    relationship_mgr, last_contact_date, preferred_channel,
    push_opt_in, avg_session_duration_30d, push_ignore_streak,
    days_since_last_contact, product_usage_score, updated_at
) VALUES (
             '30000003-0000-0000-0000-000000000001'::uuid,
             '10000003-0000-0000-0000-000000000001'::uuid,
             'retail', '["checking"]'::jsonb,
             FALSE, FALSE, 58.00, 0.720,
             '99000003-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '75 days',
             'app', TRUE,
             320,  -- baseline sessione
             4,    -- ② push_ignore_streak >= 3 ✓
             75, 0.350, NOW()
         );

-- App events storici (> 14gg fa): sessioni lunghe, feature miste
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES
      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'investimenti/fondi',
       gen_random_uuid(), 380, NOW() - INTERVAL '42 days',
       'ios', TRUE, 'commercial', 6, FALSE),

      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'offerte/prodotti',
       gen_random_uuid(), 340, NOW() - INTERVAL '35 days',
       'ios', FALSE, 'commercial', 7, FALSE),

      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'dashboard/home',
       gen_random_uuid(), 310, NOW() - INTERVAL '22 days',
       'ios', FALSE, 'essential', 4, FALSE);

-- App events recenti (< 14gg): SOLO essential, sessioni corte → ① ③
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES
      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'dashboard/home',
       gen_random_uuid(), 185, NOW() - INTERVAL '12 days',
       'ios', FALSE, 'essential', 2, FALSE),

      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'movimenti/lista',
       gen_random_uuid(), 170, NOW() - INTERVAL '9 days',
       'ios', FALSE, 'essential', 1, FALSE),

      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'dashboard/home',
       gen_random_uuid(), 175, NOW() - INTERVAL '6 days',
       'ios', FALSE, 'essential', 1, FALSE),

      (gen_random_uuid(),
       '10000003-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'bonifici/nuovo',
       gen_random_uuid(), 190, NOW() - INTERVAL '3 days',
       'ios', FALSE, 'essential', 2, FALSE);

-- Transazioni ordinarie per cold start (entro TTL 14gg di transactions_raw)
-- Portano w365Count a 5 → sopra COLD_START_THRESHOLD=5 → isColdStart=0
-- NON disturbano la logica P-03 (sepa_dd/pos, nessun segnale churn)
-- L'ultima a NOW() garantisce che smash-batch veda il cliente al prossimo tick
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
      (gen_random_uuid(),
       '20000003-0000-0000-0000-000000000001'::uuid,
       '10000003-0000-0000-0000-000000000001'::uuid,
       3000.00, 'EUR', 'salary_income', 'sepa_dd',
       'IT00EMPLOYER0000000001', NULL,
       NOW() - INTERVAL '12 days', (NOW() - INTERVAL '12 days')::date,
       NULL, TRUE, 'ordinary_baseline'),

      (gen_random_uuid(),
       '20000003-0000-0000-0000-000000000001'::uuid,
       '10000003-0000-0000-0000-000000000001'::uuid,
       -850.00, 'EUR', 'utilities', 'sepa_dd',
       'IT00UTILITY000000000001', NULL,
       NOW() - INTERVAL '10 days', (NOW() - INTERVAL '10 days')::date,
       NULL, TRUE, 'ordinary_baseline'),

      (gen_random_uuid(),
       '20000003-0000-0000-0000-000000000001'::uuid,
       '10000003-0000-0000-0000-000000000001'::uuid,
       -200.00, 'EUR', 'grocery', 'pos',
       NULL, '50000003-0000-0000-0000-000000000001'::uuid,
       NOW() - INTERVAL '7 days', (NOW() - INTERVAL '7 days')::date,
       NULL, FALSE, 'ordinary_baseline'),

-- Trigger smash-batch — ingested_at=NOW() → smash-batch vede il cliente
      (gen_random_uuid(),
       '20000003-0000-0000-0000-000000000001'::uuid,
       '10000003-0000-0000-0000-000000000001'::uuid,
       -5.00, 'EUR', 'grocery', 'pos',
       NULL, '50000003-0000-0000-0000-000000000001'::uuid,
       NOW(), NOW()::date,
       NULL, FALSE, 'ordinary_baseline');

-- Transazione verso nuovo IBAN (segnale esplorativo pre-churn)
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES (
             gen_random_uuid(),
             '20000003-0000-0000-0000-000000000001'::uuid,
             '10000003-0000-0000-0000-000000000001'::uuid,
             -500.00, 'EUR', 'internal_transfer', 'wire',
             'IT00NEWIBAN0NEVERSEEN01', NULL,
             NOW() - INTERVAL '5 days', (NOW() - INTERVAL '5 days')::date,
             NULL, FALSE, 'churn_emotional_distance'
         );

-- Market data: neutro — nessun contesto amplificante per churn
INSERT INTO smash_own.market_data (
    record_id, data_type, metric_name, value, previous_value,
    recorded_at, source
) VALUES
    (gen_random_uuid(), 'ecb_rate', 'ecb_deposit_rate',
     3.25000, 3.25000, NOW() - INTERVAL '1 day', 'synthetic');

-- ⏱️  PAUSA ~5 MINUTI — aspetta smash-batch → isColdStart=0
-- Verifica: Kafka UI → customer.baselines → isColdStart=0 per questo cliente
-- Poi esegui il trigger qui sotto.

-- Evento trigger → P-03-PHASE1
-- session_duration=175s → drop 45.3% > 30% → ③
-- feature_category='essential' → contribuisce a hasFeatureNarrowing ①
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES (
             gen_random_uuid(),
             '10000003-0000-0000-0000-000000000001'::uuid,
             'screen_view', 'movimenti/lista',
             gen_random_uuid(), 175, NOW(),
             'ios', FALSE, 'essential', 1, FALSE
         );

-- ── VERIFICA ──────────────────────────────────────────────
SELECT c.customer_id, c.segment::text, cr.push_ignore_streak,
       cr.avg_session_duration_30d, cr.days_since_last_contact
FROM smash_own.customers c
         JOIN smash_own.crm_profiles cr USING (customer_id)
WHERE c.customer_id = '10000003-0000-0000-0000-000000000001'::uuid;

SELECT feature_category::text, COUNT(*) AS eventi,
       AVG(session_duration_s)::int AS avg_s, MAX(event_timestamp) AS ultima
FROM smash_own.app_events
WHERE customer_id = '10000003-0000-0000-0000-000000000001'::uuid
GROUP BY feature_category ORDER BY ultima DESC;

SELECT '① hasFeatureNarrowing' AS segnale,
       CASE WHEN (
                     SELECT COUNT(DISTINCT feature_category) FROM smash_own.app_events
                     WHERE customer_id = '10000003-0000-0000-0000-000000000001'::uuid
                       AND event_timestamp >= NOW() - INTERVAL '14 days'
                 ) = 1 THEN '✓ ATTIVO' ELSE '✗ inattivo' END AS stato
UNION ALL
SELECT '② pushIgnoreStreak >= 3',
       CASE WHEN push_ignore_streak >= 3
                THEN '✓ ATTIVO (' || push_ignore_streak || ')'
            ELSE '✗ inattivo' END
FROM smash_own.crm_profiles
WHERE customer_id = '10000003-0000-0000-0000-000000000001'::uuid
UNION ALL
SELECT '③ sessionDrop > 30% (175s vs 320s)',
       CASE WHEN (320.0 - 175.0) / 320.0 > 0.30
                THEN '✓ ATTIVO (' || round(((320.0-175.0)/320.0*100)::numeric,1) || '%)'
            ELSE '✗ inattivo' END;