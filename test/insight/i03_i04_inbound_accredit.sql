-- ============================================================
-- 13_test_i03_i04.sql — Test I-03 e I-04: Accredito anomalo INBOUND
--
-- I-03: txnAmount > counterpart.avgAmount12m * 1.20  (+20%)
-- I-04: txnAmount < counterpart.avgAmount12m * 0.80  (-20%)
--       AND paymentCount12m >= 6 AND direction = INBOUND
--
-- Strategia:
--   6 accrediti storici dallo stesso datore di lavoro ~2000 EUR
--   → counterpart.avgAmount12m ≈ 2000 EUR, paymentCount12m = 6
--   Soglia I-03: amount > 2000 * 1.20 = 2400 EUR → trigger a 2600 EUR
--   Soglia I-04: amount < 2000 * 0.80 = 1600 EUR → trigger a 1400 EUR
--
-- Sequenza:
--   1. 00_cleanup.sql
--   2. Questo script
--   3. Aspetta ~2 min (smash-batch calcola counterpart_daily_metrics)
--   4. INSERT trigger I-03 a NOW() → verifica I-03
--   5. INSERT trigger I-04 a NOW() → verifica I-04
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
    ('f0000103-0000-0000-0000-000000000103',
     'Andrea', 'Conti', '1985-11-22', 'Torino', 'CNTNDR85S22L219X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 72.0, '2016-09-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'checking', 'IT12A0000000103000000000103', 'EUR',
     5500.00, '2016-09-01', 'active', 500.00);

-- ============================================================
-- CARD
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'f1000103-0000-0000-0000-000000000103',
     'debit', '4111111111110103',
     NULL, 0.00, 1, 'active',
     '2016-09-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'retail', '["checking"]', false, false,
     72.0, 0.08, 'app', true,
     175, 0, 15, 0.65);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000103-0000-0000-0000-000000000103',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- 6 ACCREDITI STORICI dallo stesso datore di lavoro (wire INBOUND)
--
-- counterpart: 'azienda_spa' → token HMAC costante
-- importi: variazione ±3% intorno a 2000 EUR → avg ≈ 2000 EUR
-- direction = INBOUND (amount > 0)
-- card_id = NULL perché wire non ha carta
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    -- mese -8 (stipendio)
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     1980.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '240 days', CURRENT_DATE - 240, true, NULL),

    -- mese -7
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     2020.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '210 days', CURRENT_DATE - 210, true, NULL),

    -- mese -6
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     1995.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '180 days', CURRENT_DATE - 180, true, NULL),

    -- mese -5
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     2010.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '150 days', CURRENT_DATE - 150, true, NULL),

    -- mese -4
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     1990.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '120 days', CURRENT_DATE - 120, true, NULL),

    -- mese -3
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     2005.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW() - INTERVAL '90 days', CURRENT_DATE - 90, true, NULL);

-- ============================================================
-- TRANSAZIONI EXTRA — cold start (>= 5 totali con card_id)
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'f2000103-0000-0000-0000-000000000103',
     -65.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, NULL),

    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'f2000103-0000-0000-0000-000000000103',
     -50.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '12 days', CURRENT_DATE - 12, false, NULL),

    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     'f2000103-0000-0000-0000-000000000103',
     -75.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '5 days', CURRENT_DATE - 5, false, NULL);

-- ============================================================
-- TRIGGER I-03 — accredito ALTO (esegui a NOW() dopo ~2 min)
--
-- 2600 > 2000 * 1.20 (= 2400) ✅
-- paymentCount12m = 6 ✅
-- direction = INBOUND (amount > 0) ✅
--
INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000103-0000-0000-0000-000000000103',
     'f0000103-0000-0000-0000-000000000103',
     NULL,
     2600.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
     NOW(), CURRENT_DATE, true, 'i03_trigger');

-- ============================================================
-- TRIGGER I-04 — accredito BASSO (esegui dopo aver verificato I-03)
--
-- 1400 < 2000 * 0.80 (= 1600) ✅
-- paymentCount12m = 6 ✅
-- direction = INBOUND ✅
--
-- INSERT INTO smash_own.transactions
--     (transaction_id, account_id, customer_id, card_id,
--      amount, currency, merchant_category, channel, counterpart,
--      transaction_date, value_date, is_recurring, pattern_phase)
-- VALUES
--     (gen_random_uuid(),
--      'f1000103-0000-0000-0000-000000000103',
--      'f0000103-0000-0000-0000-000000000103',
--      NULL,
--      1400.00, 'EUR', 'salary_income', 'wire', 'azienda_spa',
--      NOW(), CURRENT_DATE, true, 'i04_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CounterpartProfile dopo smash-batch:
--   direction       = INBOUND (avg > 0)
--   paymentCount12m = 6 ✅
--   avgAmount12m    ≈ 2000 EUR
--
-- Trigger I-03 (+2600 EUR):
--   2600 > 2000 * 1.20 (= 2400) ✅ → detectedPatterns: ["I-03:<token>"]
--
-- Trigger I-04 (+1400 EUR):
--   1400 < 2000 * 0.80 (= 1600) ✅ → detectedPatterns: ["I-04:<token>"]
-- ============================================================
SELECT
    'Setup I-03/I-04 completato' AS status,
    'f0000103-0000-0000-0000-000000000103' AS customer_id,
    'avg atteso ~2000 EUR su 6 accrediti | soglia I-03: 2400 | soglia I-04: 1600' AS logica,
    'Aspetta ~2 min smash-batch, poi esegui i trigger commentati in sequenza' AS prossimo_passo;