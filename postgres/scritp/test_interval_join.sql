-- ============================================================
-- test_interval_join.sql — Dataset per test interval join hot path
-- POC C: Real-Time Event Mesh con AI Enrichment
--
-- Casi di test:
--   CASO A (cust 0001): transazione + app event a 2s → DEVE joinare
--   CASO B (cust 0002): transazione + app event a 3s → DEVE joinare
--   CASO C (cust 0003): solo transazione (bonifico) → txnOnly
--   CASO D (cust 0004): solo app event (navigazione) → appOnly
--   CASO E (cust 0005): transazione + app event a 10s → NON joina (fuori finestra)
--   CASO F (cust 0006): transazione + app event a 6s → borderline (fuori finestra)
--
-- Uso:
--   psql -h localhost -U smash_app -d smash -f test_interval_join.sql
--
-- ATTENZIONE: non trunca — inserisce su dati esistenti.
-- Se vuoi ripartire pulito esegui prima test_dataset.sql
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- CUSTOMERS — 6 clienti di test per interval join
-- ============================================================

INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES
    ('a2a2a2a2-0001-0001-0001-000000000001', 'Test',  'JoinA',  '1985-01-01', 'Milano', 'TSTJNA85A01F205A', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true),
    ('a2a2a2a2-0002-0002-0002-000000000002', 'Test',  'JoinB',  '1985-01-02', 'Roma',   'TSTJNB85A02H501B', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true),
    ('a2a2a2a2-0003-0003-0003-000000000003', 'Test',  'TxnOnly','1985-01-03', 'Torino', 'TSTTXN85A03L219C', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true),
    ('a2a2a2a2-0004-0004-0004-000000000004', 'Test',  'AppOnly','1985-01-04', 'Napoli', 'TSTAPP85A04F839D', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true),
    ('a2a2a2a2-0005-0005-0005-000000000005', 'Test',  'NoJoin', '1985-01-05', 'Firenze','TSTNJ085A05D612E', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true),
    ('a2a2a2a2-0006-0006-0006-000000000006', 'Test',  'Border', '1985-01-06', 'Bologna','TSTBDR85A06A944F', 'retail', 'ordinary', NULL, 'ordinary', 'low', 50.00, '2020-01-01', true);


-- ============================================================
-- ACCOUNTS
-- ============================================================

INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency, current_balance, opened_date, status)
VALUES
    ('b2b2b2b2-0001-0001-0001-000000000001', 'a2a2a2a2-0001-0001-0001-000000000001', 'checking', 'IT12B0000000001000000000001', 'EUR', 5000.00, '2020-01-01', 'active'),
    ('b2b2b2b2-0002-0002-0002-000000000002', 'a2a2a2a2-0002-0002-0002-000000000002', 'checking', 'IT12B0000000002000000000002', 'EUR', 5000.00, '2020-01-01', 'active'),
    ('b2b2b2b2-0003-0003-0003-000000000003', 'a2a2a2a2-0003-0003-0003-000000000003', 'checking', 'IT12B0000000003000000000003', 'EUR', 5000.00, '2020-01-01', 'active'),
    ('b2b2b2b2-0004-0004-0004-000000000004', 'a2a2a2a2-0004-0004-0004-000000000004', 'checking', 'IT12B0000000004000000000004', 'EUR', 5000.00, '2020-01-01', 'active'),
    ('b2b2b2b2-0005-0005-0005-000000000005', 'a2a2a2a2-0005-0005-0005-000000000005', 'checking', 'IT12B0000000005000000000005', 'EUR', 5000.00, '2020-01-01', 'active'),
    ('b2b2b2b2-0006-0006-0006-000000000006', 'a2a2a2a2-0006-0006-0006-000000000006', 'checking', 'IT12B0000000006000000000006', 'EUR', 5000.00, '2020-01-01', 'active');


-- ============================================================
-- CRM_PROFILES
-- ============================================================

INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact, product_usage_score)
VALUES
    ('e2e2e2e2-0001-0001-0001-000000000001', 'a2a2a2a2-0001-0001-0001-000000000001', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500),
    ('e2e2e2e2-0002-0002-0002-000000000002', 'a2a2a2a2-0002-0002-0002-000000000002', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500),
    ('e2e2e2e2-0003-0003-0003-000000000003', 'a2a2a2a2-0003-0003-0003-000000000003', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500),
    ('e2e2e2e2-0004-0004-0004-000000000004', 'a2a2a2a2-0004-0004-0004-000000000004', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500),
    ('e2e2e2e2-0005-0005-0005-000000000005', 'a2a2a2a2-0005-0005-0005-000000000005', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500),
    ('e2e2e2e2-0006-0006-0006-000000000006', 'a2a2a2a2-0006-0006-0006-000000000006', 'retail', '["checking"]', false, false, 50.00, 0.100, 'app', true, 120, 0, 10, 0.500);


-- ============================================================
-- TRANSACTIONS + APP_EVENTS — timestamp vicini per testare il join
--
-- Strategia: tutti i timestamp sono NOW() con offset in secondi
-- In questo modo quando Debezium li cattura arrivano a Flink
-- con timestamp reali e il watermark funziona correttamente.
-- ============================================================

-- ── CASO A: transazione + app event a 2 secondi — DEVE joinare ──────────
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    ('f2f2f2f2-0001-0001-0001-000000000001',
     'b2b2b2b2-0001-0001-0001-000000000001',
     'a2a2a2a2-0001-0001-0001-000000000001',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW(),
     CURRENT_DATE, false, 'ordinary_baseline');

INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES
    ('c2c2c2c2-0001-0001-0001-000000000001',
     'a2a2a2a2-0001-0001-0001-000000000001',
     'screen_view', 'pagamenti/conferma', gen_random_uuid(),
     45, NOW() + INTERVAL '2 seconds',
     'ios', false, 'essential', 2, false);


-- ── CASO B: transazione + app event a 3 secondi — DEVE joinare ──────────
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    ('f2f2f2f2-0002-0002-0002-000000000002',
     'b2b2b2b2-0002-0002-0002-000000000002',
     'a2a2a2a2-0002-0002-0002-000000000002',
     -120.00, 'EUR', 'restaurant_cafe', 'pos', NULL,
     NOW(),
     CURRENT_DATE, false, 'ordinary_baseline');

INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES
    ('c2c2c2c2-0002-0002-0002-000000000002',
     'a2a2a2a2-0002-0002-0002-000000000002',
     'screen_view', 'movimenti', gen_random_uuid(),
     60, NOW() + INTERVAL '3 seconds',
     'ios', false, 'essential', 3, false);


-- ── CASO C: solo transazione (bonifico) — txnOnly ────────────────────────
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    ('f2f2f2f2-0003-0003-0003-000000000003',
     'b2b2b2b2-0003-0003-0003-000000000003',
     'a2a2a2a2-0003-0003-0003-000000000003',
     -500.00, 'EUR', 'wire', 'wire', 'IT99C0000099003000000000001',
     NOW(),
     CURRENT_DATE, false, 'ordinary_baseline');


-- ── CASO D: solo app event (navigazione) — appOnly ───────────────────────
INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES
    ('c2c2c2c2-0004-0004-0004-000000000004',
     'a2a2a2a2-0004-0004-0004-000000000004',
     'screen_view', 'investimenti/fondi', gen_random_uuid(),
     180, NOW(),
     'ios', true, 'commercial', 5, false);


-- ── CASO E: transazione + app event a 10 secondi — NON joina ────────────
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    ('f2f2f2f2-0005-0005-0005-000000000005',
     'b2b2b2b2-0005-0005-0005-000000000005',
     'a2a2a2a2-0005-0005-0005-000000000005',
     -80.00, 'EUR', 'grocery', 'pos', NULL,
     NOW(),
     CURRENT_DATE, false, 'ordinary_baseline');

INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES
    ('c2c2c2c2-0005-0005-0005-000000000005',
     'a2a2a2a2-0005-0005-0005-000000000005',
     'screen_view', 'home', gen_random_uuid(),
     90, NOW() + INTERVAL '10 seconds',
     'ios', false, 'essential', 2, false);


-- ── CASO F: transazione + app event a 6 secondi — borderline, NON joina ─
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    ('f2f2f2f2-0006-0006-0006-000000000006',
     'b2b2b2b2-0006-0006-0006-000000000006',
     'a2a2a2a2-0006-0006-0006-000000000006',
     -35.00, 'EUR', 'transport', 'pos', NULL,
     NOW(),
     CURRENT_DATE, false, 'ordinary_baseline');

INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES
    ('c2c2c2c2-0006-0006-0006-000000000006',
     'a2a2a2a2-0006-0006-0006-000000000006',
     'screen_view', 'saldo', gen_random_uuid(),
     30, NOW() + INTERVAL '6 seconds',
     'ios', false, 'essential', 1, false);


-- ============================================================
-- VERIFICA — cosa aspettarsi nel log Flink
-- ============================================================
SELECT
    'CASO A - JOIN atteso'   AS test, COUNT(*) AS righe FROM smash_own.transactions WHERE customer_id = 'a2a2a2a2-0001-0001-0001-000000000001'
UNION ALL SELECT 'CASO B - JOIN atteso',   COUNT(*) FROM smash_own.transactions WHERE customer_id = 'a2a2a2a2-0002-0002-0002-000000000002'
UNION ALL SELECT 'CASO C - txnOnly atteso',COUNT(*) FROM smash_own.transactions WHERE customer_id = 'a2a2a2a2-0003-0003-0003-000000000003'
UNION ALL SELECT 'CASO D - appOnly atteso',COUNT(*) FROM smash_own.app_events    WHERE customer_id = 'a2a2a2a2-0004-0004-0004-000000000004'
UNION ALL SELECT 'CASO E - NO join atteso',COUNT(*) FROM smash_own.transactions  WHERE customer_id = 'a2a2a2a2-0005-0005-0005-000000000005'
UNION ALL SELECT 'CASO F - NO join atteso',COUNT(*) FROM smash_own.transactions  WHERE customer_id = 'a2a2a2a2-0006-0006-0006-000000000006';