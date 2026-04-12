-- ============================================================
-- 14_test_i13.sql — Test I-13: Nuovo abbonamento rilevato
--
-- Pattern: counterpart.isRecurring = true
--          AND counterpart.firstSeenDate >= now() - 180gg (6 mesi)
--          AND counterpart.paymentCount12m >= 3
--
-- Strategia:
--   Abbonamento attivato 60 giorni fa (< 6 mesi → "nuovo")
--   3 pagamenti regolari da allora (~15 EUR/mese, stesso merchant)
--   → isRecurring = true, firstSeenDate = ~60gg fa ✅
--   Trigger: quarta transazione verso stesso merchant → I-13 scatta
--
-- Sequenza:
--   1. 00_cleanup.sql
--   2. Questo script
--   3. Aspetta ~2 min (smash-batch calcola isRecurring + firstSeenDate)
--   4. INSERT trigger a NOW() → verifica I-13
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
    ('f0000113-0000-0000-0000-000000000113',
     'Laura', 'Marino', '1991-04-08', 'Napoli', 'MRNLRA91D48F839X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 61.0, '2019-06-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     'checking', 'IT12A0000000113000000000113', 'EUR',
     3500.00, '2019-06-01', 'active', 500.00);

-- ============================================================
-- CARD
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     'f1000113-0000-0000-0000-000000000113',
     'debit', '4111111111110113',
     NULL, 0.00, 1, 'active',
     '2019-06-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     'retail', '["checking"]', false, false,
     61.0, 0.12, 'app', true,
     145, 0, 18, 0.55);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000113-0000-0000-0000-000000000113',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- 3 PAGAMENTI ABBONAMENTO — nuovo merchant attivato 60 giorni fa
--
-- merchant: 'streaming_plus_srl' → token HMAC costante
-- importo: ~15 EUR (tipico abbonamento streaming)
-- intervallo: ~30 giorni → isRecurring = true
-- firstSeenDate = ~60gg fa → dentro finestra 6 mesi ✅
-- canale: sepa_dd → isSubscription = true (bonus: scatta anche I-14 se aumenta)
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    -- primo pagamento: 60 giorni fa (firstSeenDate)
    (gen_random_uuid(),
     'f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     NULL,
     -14.99, 'EUR', 'subscriptions', 'sepa_dd', 'streaming_plus_srl',
     NOW() - INTERVAL '60 days', CURRENT_DATE - 60, true, NULL),

    -- secondo pagamento: 30 giorni fa
    (gen_random_uuid(),
     'f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     NULL,
     -14.99, 'EUR', 'subscriptions', 'sepa_dd', 'streaming_plus_srl',
     NOW() - INTERVAL '30 days', CURRENT_DATE - 30, true, NULL),

    -- terzo pagamento: 2 giorni fa (paymentCount12m = 3 ✅)
    (gen_random_uuid(),
     'f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     NULL,
     -14.99, 'EUR', 'subscriptions', 'sepa_dd', 'streaming_plus_srl',
     NOW() - INTERVAL '2 days', CURRENT_DATE - 2, true, NULL);

-- ============================================================
-- TRANSAZIONI EXTRA — cold start
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     'f2000113-0000-0000-0000-000000000113',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '22 days', CURRENT_DATE - 22, false, NULL),

    (gen_random_uuid(),
     'f1000113-0000-0000-0000-000000000113',
     'f0000113-0000-0000-0000-000000000113',
     'f2000113-0000-0000-0000-000000000113',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, NULL);

-- ============================================================
-- TRIGGER I-13 — quarta transazione verso stesso merchant
-- (esegui a NOW() dopo ~2 min)
--
-- Dopo smash-batch:
--   isRecurring    = true (3 pagamenti, intervallo ~30gg, CV basso) ✅
--   firstSeenDate  = ~60gg fa < 180gg ✅
--   paymentCount12m = 3 ✅
--
-- INSERT INTO smash_own.transactions
--     (transaction_id, account_id, customer_id, card_id,
--      amount, currency, merchant_category, channel, counterpart,
--      transaction_date, value_date, is_recurring, pattern_phase)
-- VALUES
--     (gen_random_uuid(),
--      'f1000113-0000-0000-0000-000000000113',
--      'f0000113-0000-0000-0000-000000000113',
--      NULL,
--      -14.99, 'EUR', 'subscriptions', 'sepa_dd', 'streaming_plus_srl',
--      NOW(), CURRENT_DATE, true, 'i13_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CounterpartProfile dopo smash-batch:
--   isRecurring     = true ✅
--   isSubscription  = true ✅ (cv_amount < 0.15, canale sepa_dd)
--   paymentCount12m = 3 ✅
--   firstSeenDate   = ~60gg fa → dentro 180gg ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000113-0000-0000-0000-000000000113
--   detectedPatterns: ["I-13:<token_streaming_plus_srl>"]
-- ============================================================
SELECT
    'Setup I-13 completato' AS status,
    'f0000113-0000-0000-0000-000000000113' AS customer_id,
    'abbonamento streaming_plus_srl attivato 60gg fa, 3 pagamenti da 14.99' AS logica,
    'Aspetta ~2 min smash-batch, poi esegui trigger commentato' AS prossimo_passo;