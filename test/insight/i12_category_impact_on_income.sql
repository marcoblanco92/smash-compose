-- ============================================================
-- 07_test_i12.sql — Test I-12: Incidenza categoria su reddito
--
-- Insight: catAmounts30d[cat] / estimatedMonthlyIncome > 0.30
--          AND estimatedMonthlyIncome > 0
--          AND catAmounts30d[cat] > 0
--          AND cat NOT IN (internal_transfer, salary_income, b2b_transfer, investment)
--
-- Strategia:
--   estimatedMonthlyIncome = accredito wire/instant negli ultimi 30gg = 2000 EUR
--   catAmounts30d[dining] = 700 EUR (3 transazioni)
--   700 / 2000 = 35% > 30% → I-12:dining:35pct scatta
--
-- Sequenza:
--   1. Esegui 00_cleanup.sql
--   2. Esegui questo script
--   3. Aspetta ~2 min (smash-batch calcola estimatedMonthlyIncome)
--   4. Inserisci transazione trigger dining → verifica events.enriched
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- CUSTOMER
-- ============================================================
INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES ('f0000012-0000-0000-0000-000000000012',
        'Sara', 'Vitali', '1990-06-20', 'Roma', 'VTLSRA90H60H501Z',
        'retail', 'ordinary', NULL, 'ordinary',
        'low', 60.0, '2020-03-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES ('f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'checking', 'IT12A0000000012000000000012', 'EUR',
        3200.00, '2020-03-01', 'active', 500.00);

-- ============================================================
-- CARD
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES ('f2000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f1000012-0000-0000-0000-000000000012',
        'debit', '4111111111110012',
        NULL, 0.00, 1, 'active',
        '2020-03-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES ('f3000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'retail', '[
    "checking"
  ]', false, false,
        60.0, 0.15, 'app', true,
        150, 0, 20, 0.55);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES ('f4000012-0000-0000-0000-000000000012',
        'ecb_rate', 'ecb_rate', 4.50, 4.25,
        NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- ACCREDITO STIPENDIO — popola estimatedMonthlyIncome
--
-- smash-batch calcola: income_sum su wire/instant con amount > 0
-- negli ultimi 30gg → estimatedMonthlyIncome = 2000 EUR
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        NULL, -- wire non ha card_id
        2000.00, 'EUR', 'salary_income', 'wire', 'datore_lavoro_spa',
        NOW() - INTERVAL '25 days', CURRENT_DATE - 25, true, NULL);

-- ============================================================
-- TRANSAZIONI DINING — ultimi 30gg
--
-- catAmounts30d[dining] = 700 EUR
-- 700 / 2000 = 0.35 > 0.30 → I-12:dining:35pct
--
-- Il CEP valuta ad ogni transazione dining:
-- dopo la terza (count = 3) la condizione è soddisfatta
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -200.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, 'i12_dining'),

       (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -150.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '12 days', CURRENT_DATE - 12, false, 'i12_dining'),

       (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -50.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '11days', CURRENT_DATE - 11, false, 'i12_dining'),
       (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -50.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, 'i12_dining');

-- Terza transazione TRIGGER: totale = 700, 700/2000 = 35% > 30% → I-12 scatta
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -250.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '3 days', CURRENT_DATE - 3, false, 'i12_dining'),

       (gen_random_uuid(),
        'f1000012-0000-0000-0000-000000000012',
        'f0000012-0000-0000-0000-000000000012',
        'f2000012-0000-0000-0000-000000000012',
        -150.00, 'EUR', 'dining', 'pos', NULL,
        NOW() - INTERVAL '1 days', CURRENT_DATE - 1, false, 'i12_dining')
;

-- ============================================================
-- VERIFICA ATTESA
--
-- estimatedMonthlyIncome     = 2000 EUR (accredito wire)
-- catAmounts30d[dining]      = 700 EUR
-- catCounts30d[dining]       = 3
-- incidenza                  = 700 / 2000 = 35%
--
-- Condizione CEP:
--   35% > 30% ✅
--   estimatedMonthlyIncome > 0 ✅
--   catAmounts30d > 0 ✅
--   categoria != escluse ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000012-0000-0000-0000-000000000012
--   detectedPatterns: ["I-12:dining:35pct"]
--                     (il pct esatto dipende dal formato nel CepEvaluator)
-- ============================================================
SELECT 'Setup I-12 completato'                                                                 AS status,
       'f0000012-0000-0000-0000-000000000012'                                                  AS customer_id,
       'Attendi ~2 min per smash-batch (estimatedMonthlyIncome), poi verifica events.enriched' AS prossimo_passo;