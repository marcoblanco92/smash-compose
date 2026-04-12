-- ============================================================
-- 06_test_i07.sql — Test I-07: Aumento spese categoria
--
-- Insight: catAmounts30d[cat] > catAvgAmounts90d[cat] * 1.20
--          AND catCount30d[cat] >= 3
--          AND cat NOT IN (internal_transfer, salary_income, b2b_transfer, investment)
--
-- Strategia:
--   Cliente retail con storico grocery nei mesi -90/-30:
--     avg90d grocery ≈ 197 EUR/mese
--   Ultimi 30gg: 3 transazioni grocery totale 290 EUR (> 197 * 1.20 = 236)
--   → atteso: I-07:grocery nel detectedPatterns
--
-- Sequenza:
--   1. Esegui 00_cleanup.sql
--   2. Esegui questo script
--   3. Aspetta ~2 min (smash-batch pubblica baseline aggiornata)
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
    ('f0000007-0000-0000-0000-000000000007',
     'Matteo', 'Grandi', '1988-04-12', 'Milano', 'GRNMTT88D12F205X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 65.0, '2019-01-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'checking', 'IT12A0000000007000000000007', 'EUR',
     4500.00, '2019-01-01', 'active', 500.00);

-- ============================================================
-- CARD (debit — serve solo per FK su transactions)
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f1000007-0000-0000-0000-000000000007',
     'debit', '4111111111110007',
     NULL, 0.00, 1, 'active',
     '2019-01-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'retail', '["checking"]', false, false,
     65.0, 0.10, 'app', true,
     180, 0, 30, 0.60);

-- ============================================================
-- MARKET DATA
-- Schema: record_id, data_type, metric_name, value, previous_value,
--         recorded_at, source
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000007-0000-0000-0000-000000000007',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- TRANSAZIONI STORICHE — costruisce baseline 90d
--
-- Obiettivo: merchantCatAvgAmounts90d[grocery] ≈ 197 EUR
-- 2 mesi di storico grocery con ~195-200 EUR/mese
-- smash-batch legge daily_metrics FINAL → calcola media 90d
-- ============================================================

-- Mese -2 (da -90 a -60 giorni fa): totale 195 EUR
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -65.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '80 days', CURRENT_DATE - 80, false, NULL),

    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -70.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '72 days', CURRENT_DATE - 72, false, NULL),

    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '65 days', CURRENT_DATE - 65, false, NULL),

-- Mese -1 (da -60 a -30 giorni fa): totale 200 EUR
    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -68.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '55 days', CURRENT_DATE - 55, false, NULL),

    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -72.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '45 days', CURRENT_DATE - 45, false, NULL),

    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '35 days', CURRENT_DATE - 35, false, NULL);

-- ============================================================
-- TRANSAZIONI TRIGGER — ultimi 30 giorni
--
-- catAmounts30d[grocery] = 290 EUR > 197 * 1.20 (= 236) ✅
-- catCount30d[grocery] = 3 ✅
-- Il CEP scatta alla terza transazione (count raggiunge soglia minima)
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -95.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, 'i07_grocery'),

    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -100.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, 'i07_grocery');

-- Terza transazione: count = 3, totale = 290 → I-07:grocery scatta
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000007-0000-0000-0000-000000000007',
     'f0000007-0000-0000-0000-000000000007',
     'f2000007-0000-0000-0000-000000000007',
     -195.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '2 days', CURRENT_DATE - 2, false, 'i07_grocery');

-- ============================================================
-- VERIFICA ATTESA
--
-- merchantCatAvgAmounts90d[grocery] ≈ 197 EUR  (2 mesi storici)
-- merchantCatAmounts30d[grocery]    = 290 EUR
-- merchantCatCounts30d[grocery]     = 3
--
-- Condizione CEP:
--   290 > 197 * 1.20 (= 236.4) ✅
--   count >= 3 ✅
--   categoria != escluse ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000007-0000-0000-0000-000000000007
--   detectedPatterns: ["I-07:grocery"]
-- ============================================================
SELECT
    'Setup I-07 completato' AS status,
    'f0000007-0000-0000-0000-000000000007' AS customer_id,
    'Attendi ~2 min per smash-batch, poi verifica events.enriched' AS prossimo_passo;