-- ============================================================
-- 10_test_i17.sql — Test I-17: Possibile blocco carta
--
-- Insight: plafondAvailable / (w7.sumAmt / 7) <= 4 giorni
--          AND card.status = active
--          AND card.type = credit
--
-- Matematica:
--   plafondAvailable = 300 EUR  (plafond_limit 2000 - plafond_used 1700)
--   w7.sumAmt = 560 EUR in 7 giorni → dailySpend = 80 EUR/giorno
--   300 / 80 = 3.75 giorni <= 4 → I-17 scatta
--
-- Sequenza:
--   1. Esegui 00_cleanup.sql
--   2. Esegui questo script
--   3. Aspetta ~2 min (smash-batch cold start) + ~30s CDC cards
--   4. Inserisci transazione trigger a NOW() → verifica events.enriched
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
    ('f0000017-0000-0000-0000-000000000017',
     'Paolo', 'Gentile', '1983-11-05', 'Bologna', 'GNTPLA83S05A944X',
     'retail', 'ordinary', NULL, 'ordinary',
     'medium', 56.0, '2016-04-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'checking', 'IT12A0000000017000000000017', 'EUR',
     2500.00, '2016-04-01', 'active', 500.00);

-- ============================================================
-- CARD — credito con plafond quasi esaurito
--
-- plafond_limit = 2000 EUR
-- plafond_used  = 1700 EUR
-- plafondAvailable = 300 EUR  (calcolato da Flink: limit - used)
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f1000017-0000-0000-0000-000000000017',
     'credit', '4111111111110017',
     2000.00, 1700.00, 15, 'active',
     '2016-04-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'retail', '["checking","credit_card"]', false, false,
     56.0, 0.18, 'app', true,
     130, 0, 20, 0.50);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000017-0000-0000-0000-000000000017',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- TRANSAZIONI STORICHE — cold start protection (>= 5 totali)
-- Fuori dalla finestra w7 (> 7 giorni fa) — non influenzano w7.sumAmt
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '25 days', CURRENT_DATE - 25, false, NULL),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, NULL),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -70.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '15 days', CURRENT_DATE - 15, false, NULL),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -50.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, NULL),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -65.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '8 days', CURRENT_DATE - 8, false, NULL);

-- ============================================================
-- TRANSAZIONI ULTIMI 7 GIORNI — carica w7.sumAmt
--
-- Totale: 140 + 130 + 150 + 140 = 560 EUR
-- w7.sumAmt = 560 EUR
-- dailySpend = 560 / 7 = 80 EUR/giorno
-- giorni rimanenti = 300 / 80 = 3.75 <= 4 → I-17 scatta
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -140.00, 'EUR', 'shopping', 'pos', NULL,
     NOW() - INTERVAL '6 days', CURRENT_DATE - 6, false, 'i17_spend'),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -130.00, 'EUR', 'dining', 'pos', NULL,
     NOW() - INTERVAL '4 days', CURRENT_DATE - 4, false, 'i17_spend'),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -150.00, 'EUR', 'shopping', 'online', NULL,
     NOW() - INTERVAL '2 days', CURRENT_DATE - 2, false, 'i17_spend'),

    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -140.00, 'EUR', 'dining', 'pos', NULL,
     NOW() - INTERVAL '1 day', CURRENT_DATE - 1, false, 'i17_spend');

-- ============================================================
-- TRANSAZIONE TRIGGER — esegui a NOW() dopo ~2 min
--
-- Aspetta che:
--   1. smash-batch pubblichi baseline (cold start = false)
--   2. CDC cards → Flink aggiorni CardProfileState (plafondAvailable = 300)
--
-- Poi esegui:
--
INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000017-0000-0000-0000-000000000017',
     'f0000017-0000-0000-0000-000000000017',
     'f2000017-0000-0000-0000-000000000017',
     -20.00, 'EUR', 'grocery', 'pos', NULL,
     NOW(), CURRENT_DATE, false, 'i17_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CardProfileState:
--   cardType         = credit ✅
--   status           = active ✅
--   plafondLimit     = 2000 EUR
--   plafondUsed      = 1700 EUR
--   plafondAvailable = 300 EUR ✅
--
-- w7 dopo le 4 transazioni degli ultimi 7 giorni:
--   w7.sumAmt = 560 EUR
--   dailySpend = 560 / 7 = 80 EUR/giorno
--   300 / 80 = 3.75 giorni <= 4 ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000017-0000-0000-0000-000000000017
--   detectedPatterns: ["I-16:f2000017-...", "I-17:f2000017-...:3d"]
--   (I-16 scatta anche perché plafond ancora all'85%)
-- ============================================================
SELECT
    'Setup I-17 completato' AS status,
    'f0000017-0000-0000-0000-000000000017' AS customer_id,
    'f2000017-0000-0000-0000-000000000017' AS card_id,
    '300 EUR disponibili / (560 EUR / 7 giorni) = 3.75 giorni <= 4' AS logica,
    'Attendi ~2 min smash-batch + ~30s CDC, poi esegui INSERT trigger' AS prossimo_passo;