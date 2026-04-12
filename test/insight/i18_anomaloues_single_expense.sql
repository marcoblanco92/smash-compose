-- ============================================================
-- 08_test_i18.sql — Test I-18: Uscita singola anomala
--
-- Insight: abs(amount) > abs(w90MinAmt) * 1.5
--          AND amount < 0
--          AND w90MinAmt != 0
--
-- Strategia:
--   Storico 90d: transazioni con max uscita singola = -200 EUR
--   → w90MinAmt = -200 EUR (valore più negativo = spesa singola più alta)
--   Transazione trigger: -350 EUR
--   abs(-350) = 350 > abs(-200) * 1.5 (= 300) → I-18 scatta
--
-- Sequenza:
--   1. Esegui 00_cleanup.sql
--   2. Esegui questo script
--   3. Aspetta ~2 min (smash-batch calcola w90MinAmt dalla baseline)
--   4. Verifica PreEnrichedEvent su events.enriched
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- CUSTOMER
-- ============================================================
INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES
    ('f0000018-0000-0000-0000-000000000018',
     'Luca', 'Ferrara', '1985-09-15', 'Torino', 'FRRLCU85P15L219X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 62.0, '2018-05-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'checking', 'IT12A0000000018000000000018', 'EUR',
     5000.00, '2018-05-01', 'active', 500.00);

-- ============================================================
-- CARD
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f1000018-0000-0000-0000-000000000018',
     'debit', '4111111111110018',
     NULL, 0.00, 1, 'active',
     '2018-05-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'retail', '["checking"]', false, false,
     62.0, 0.10, 'app', true,
     160, 0, 25, 0.58);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000018-0000-0000-0000-000000000018',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- TRANSAZIONI STORICHE — costruisce w90MinAmt = -200 EUR
--
-- Inseriamo transazioni regolari negli ultimi 90 giorni.
-- La più alta in valore assoluto è -200 EUR → w90MinAmt = -200
-- smash-batch usa minState(amt) in daily_metrics → cattura il minimo
--
-- Soglia trigger: abs(-200) * 1.5 = 300 EUR
-- Servono almeno 5 transazioni totali per uscire dal cold start
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    -- Transazioni ordinarie mese -2 (spese tipiche 50-120 EUR)
    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -80.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '75 days', CURRENT_DATE - 75, false, NULL),

    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -120.00, 'EUR', 'dining', 'pos', NULL,
     NOW() - INTERVAL '68 days', CURRENT_DATE - 68, false, NULL),

    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -50.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '60 days', CURRENT_DATE - 60, false, NULL),

    -- Transazioni ordinarie mese -1
    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -90.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '50 days', CURRENT_DATE - 50, false, NULL),

    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -150.00, 'EUR', 'electronics', 'online', NULL,
     NOW() - INTERVAL '40 days', CURRENT_DATE - 40, false, NULL),

    -- Transazione più alta dello storico: -200 EUR → w90MinAmt = -200
    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -200.00, 'EUR', 'electronics', 'online', NULL,
     NOW() - INTERVAL '35 days', CURRENT_DATE - 35, false, NULL),

    -- Transazioni ultimi 30gg (ordinarie, sotto soglia)
    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -75.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, NULL),

    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -100.00, 'EUR', 'dining', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, NULL);

-- ============================================================
-- TRANSAZIONE TRIGGER — anomala
--
-- -350 EUR: abs(350) > abs(-200) * 1.5 (= 300) → I-18 scatta
-- Categoria electronics per chiarezza (non è rilevante per I-18)
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000018-0000-0000-0000-000000000018',
     'f0000018-0000-0000-0000-000000000018',
     'f2000018-0000-0000-0000-000000000018',
     -122350.00, 'EUR', 'electronics', 'online', NULL,
     NOW(), CURRENT_DATE , false, 'i18_anomaly');

-- ============================================================
-- VERIFICA ATTESA
--
-- w90MinAmt (da baseline OLAP) = -200 EUR
-- amount trigger               = -350 EUR
-- abs(-350) = 350 > abs(-200) * 1.5 (= 300) ✅
-- amount < 0 ✅
-- w90MinAmt != 0 ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000018-0000-0000-0000-000000000018
--   detectedPatterns: ["I-18"]
-- ============================================================
SELECT
    'Setup I-18 completato' AS status,
    'f0000018-0000-0000-0000-000000000018' AS customer_id,
    'w90MinAmt atteso = -200 EUR, trigger = -350 EUR, soglia = 300 EUR' AS logica,
    'Attendi ~2 min per smash-batch, poi verifica events.enriched' AS prossimo_passo;