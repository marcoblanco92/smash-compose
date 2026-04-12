-- ============================================================
-- 15_test_i14.sql — Test I-14: Aumento costo abbonamento
--
-- Pattern: counterpart.isSubscription = true
--          AND abs(txnAmount) > abs(counterpart.avgAmount12m) * 1.15
--          AND counterpart.paymentCount12m >= 3
--
-- Strategia:
--   Abbonamento consolidato da 8 mesi ~10 EUR/mese
--   → isSubscription = true, paymentCount12m = 6, avg ≈ 10 EUR
--   Trigger: pagamento a 12 EUR (+20% → sopra soglia +15%)
--   abs(12) > abs(10) * 1.15 (= 11.50) ✅ → I-14 scatta
--
-- Nota: usiamo un abbonamento esistente da > 6 mesi
--       così I-13 NON scatta (firstSeenDate > 180gg)
--       e verifichiamo I-14 in isolamento.
--
-- Sequenza:
--   1. 00_cleanup.sql
--   2. Questo script
--   3. Aspetta ~2 min (smash-batch calcola isSubscription + avgAmount)
--   4. INSERT trigger a NOW() → verifica I-14
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
    ('f0000114-0000-0000-0000-000000000114',
     'Stefano', 'Levi', '1978-09-03', 'Milano', 'LVISTF78P03F205X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 66.0, '2015-11-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     'checking', 'IT12A0000000114000000000114', 'EUR',
     4200.00, '2015-11-01', 'active', 500.00);

-- ============================================================
-- CARD
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     'f1000114-0000-0000-0000-000000000114',
     'debit', '4111111111110114',
     NULL, 0.00, 1, 'active',
     '2015-11-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     'retail', '["checking"]', false, false,
     66.0, 0.09, 'app', true,
     155, 0, 22, 0.58);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000114-0000-0000-0000-000000000114',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- 6 PAGAMENTI ABBONAMENTO STORICI — consolidato da > 6 mesi
--
-- merchant: 'cloud_storage_srl' → token HMAC costante
-- importo: ~10 EUR fisso (cv_amount ≈ 0 → isSubscription = true)
-- firstSeenDate = ~240gg fa → FUORI dai 180gg → I-13 NON scatta
-- intervallo: ~30gg → isRecurring = true
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    -- mese -8 (firstSeenDate)
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '240 days', CURRENT_DATE - 240, true, NULL),

    -- mese -7
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '210 days', CURRENT_DATE - 210, true, NULL),

    -- mese -6
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '180 days', CURRENT_DATE - 180, true, NULL),

    -- mese -5
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '150 days', CURRENT_DATE - 150, true, NULL),

    -- mese -4
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '120 days', CURRENT_DATE - 120, true, NULL),

    -- mese -3
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -9.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW() - INTERVAL '90 days', CURRENT_DATE - 90, true, NULL);

-- ============================================================
-- TRANSAZIONI EXTRA — cold start
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     'f2000114-0000-0000-0000-000000000114',
     -58.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '18 days', CURRENT_DATE - 18, false, NULL),

    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     'f2000114-0000-0000-0000-000000000114',
     -62.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '7 days', CURRENT_DATE - 7, false, NULL);

-- ============================================================
-- TRIGGER I-14 — pagamento con importo aumentato
-- (esegui a NOW() dopo ~2 min)
--
-- Prezzo aumentato da 9.99 a 12.99 EUR (+30% → sopra soglia +15%)
-- abs(12.99) = 12.99 > abs(9.99) * 1.15 (= 11.49) ✅
-- isSubscription = true ✅
-- paymentCount12m = 6 ✅
-- firstSeenDate > 180gg → I-13 NON scatta ✅
--
INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000114-0000-0000-0000-000000000114',
     'f0000114-0000-0000-0000-000000000114',
     NULL,
     -12.99, 'EUR', 'subscriptions', 'sepa_dd', 'cloud_storage_srl',
     NOW(), CURRENT_DATE, true, 'i14_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CounterpartProfile dopo smash-batch:
--   isRecurring     = true ✅
--   isSubscription  = true (cv_amount ≈ 0, sepa_dd) ✅
--   paymentCount12m = 6 ✅
--   avgAmount12m    ≈ -9.99 EUR
--   firstSeenDate   = ~240gg fa → fuori 180gg → I-13 NON scatta ✅
--
-- Trigger (-12.99 EUR):
--   abs(12.99) > abs(9.99) * 1.15 (= 11.49) ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000114-0000-0000-0000-000000000114
--   detectedPatterns: ["I-14:<token_cloud_storage_srl>"]
--   (I-13 assente ✅)
-- ============================================================
SELECT
    'Setup I-14 completato' AS status,
    'f0000114-0000-0000-0000-000000000114' AS customer_id,
    'abbonamento cloud_storage 9.99/mese da 8 mesi → aumento a 12.99 (+30%)' AS logica,
    'Aspetta ~2 min smash-batch, poi esegui trigger commentato' AS prossimo_passo;