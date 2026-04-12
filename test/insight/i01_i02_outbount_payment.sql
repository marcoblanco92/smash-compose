-- ============================================================
-- 12_test_i01_i02.sql — Test I-01 e I-02: Pagamento anomalo OUTBOUND
--
-- I-01: abs(txnAmount) > abs(counterpart.avgAmount12m) * 1.20  (+20%)
-- I-02: abs(txnAmount) < abs(counterpart.avgAmount12m) * 0.80  (-20%)
--       AND paymentCount12m >= 6 AND direction = OUTBOUND
--
-- Strategia:
--   6 pagamenti storici verso stesso fornitore ~500 EUR
--   → counterpart.avgAmount12m ≈ -500 EUR, paymentCount12m = 6
--   Soglia I-01: abs(amount) > 500 * 1.20 = 600 EUR → trigger a -650 EUR
--   Soglia I-02: abs(amount) < 500 * 0.80 = 400 EUR → trigger a -350 EUR
--
-- Sequenza:
--   1. 00_cleanup.sql
--   2. Questo script
--   3. Aspetta ~2 min (smash-batch calcola counterpart_daily_metrics)
--   4. INSERT trigger I-01 a NOW() → verifica I-01
--   5. INSERT trigger I-02 a NOW() → verifica I-02
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
    ('f0000101-0000-0000-0000-000000000101',
     'Elena', 'Russo', '1982-07-14', 'Roma', 'RSSLN182L54H501X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 68.0, '2017-03-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'checking', 'IT12A0000000101000000000101', 'EUR',
     6000.00, '2017-03-01', 'active', 500.00);

-- ============================================================
-- CARD (debit — serve solo per cold start su transazioni pos)
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'f1000101-0000-0000-0000-000000000101',
     'debit', '4111111111110101',
     NULL, 0.00, 1, 'active',
     '2017-03-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'retail', '["checking"]', false, false,
     68.0, 0.10, 'app', true,
     160, 0, 20, 0.60);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000101-0000-0000-0000-000000000101',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- 6 PAGAMENTI STORICI verso stesso fornitore (wire)
--
-- counterpart: 'fornitore_regolare_srl' → token HMAC costante
-- importi: variazione ±5% intorno a 500 EUR → avg ≈ 500 EUR
-- Distribuzione su 8 mesi → paymentCount12m = 6 ✅
-- card_id = NULL perché wire non ha carta associata
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    -- mese -8
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -490.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '240 days', CURRENT_DATE - 240, true, NULL),

    -- mese -7
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -505.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '210 days', CURRENT_DATE - 210, true, NULL),

    -- mese -6
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -495.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '180 days', CURRENT_DATE - 180, true, NULL),

    -- mese -5
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -510.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '150 days', CURRENT_DATE - 150, true, NULL),

    -- mese -4
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -498.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '120 days', CURRENT_DATE - 120, true, NULL),

    -- mese -3
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -502.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW() - INTERVAL '90 days', CURRENT_DATE - 90, true, NULL);

-- ============================================================
-- TRANSAZIONI EXTRA — cold start (>= 5 con card_id valorizzato)
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'f2000101-0000-0000-0000-000000000101',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '25 days', CURRENT_DATE - 25, false, NULL),

    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'f2000101-0000-0000-0000-000000000101',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '15 days', CURRENT_DATE - 15, false, NULL),

    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     'f2000101-0000-0000-0000-000000000101',
     -70.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '8 days', CURRENT_DATE - 8, false, NULL);

-- ============================================================
-- TRIGGER I-01 — pagamento ALTO (esegui a NOW() dopo ~2 min)
--
-- abs(-650) = 650 > 500 * 1.20 (= 600) ✅
-- paymentCount12m = 6 ✅
-- direction = OUTBOUND ✅
--
-- INSERT INTO smash_own.transactions
--     (transaction_id, account_id, customer_id, card_id,
--      amount, currency, merchant_category, channel, counterpart,
--      transaction_date, value_date, is_recurring, pattern_phase)
-- VALUES
--     (gen_random_uuid(),
--      'f1000101-0000-0000-0000-000000000101',
--      'f0000101-0000-0000-0000-000000000101',
--      NULL,
--      -650.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
--      NOW(), CURRENT_DATE, true, 'i01_trigger');

-- ============================================================
-- TRIGGER I-02 — pagamento BASSO (esegui dopo aver verificato I-01)
--
-- abs(-350) = 350 < 500 * 0.80 (= 400) ✅
-- paymentCount12m = 6 ✅
-- direction = OUTBOUND ✅
--
INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000101-0000-0000-0000-000000000101',
     'f0000101-0000-0000-0000-000000000101',
     NULL,
     -350.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_regolare_srl',
     NOW(), CURRENT_DATE, true, 'i02_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CounterpartProfile dopo smash-batch:
--   direction        = OUTBOUND
--   paymentCount12m  = 6 ✅
--   avgAmount12m     ≈ -500 EUR
--
-- Trigger I-01 (-650 EUR):
--   650 > 500 * 1.20 (= 600) ✅ → detectedPatterns: ["I-01:<token>"]
--
-- Trigger I-02 (-350 EUR):
--   350 < 500 * 0.80 (= 400) ✅ → detectedPatterns: ["I-02:<token>"]
-- ============================================================
SELECT
    'Setup I-01/I-02 completato' AS status,
    'f0000101-0000-0000-0000-000000000101' AS customer_id,
    'avg atteso ~500 EUR su 6 pagamenti | soglia I-01: 600 | soglia I-02: 400' AS logica,
    'Aspetta ~2 min smash-batch, poi esegui i trigger commentati in sequenza' AS prossimo_passo;