-- ============================================================
-- 09_test_i16.sql — Test I-16: Utilizzo plafond carta alto
--
-- Insight: plafondUsed / plafondLimit > 80%
--          AND card.status = active
--          AND card.type = credit
--
-- Strategia:
--   Cliente con carta di credito: plafond_limit = 2000 EUR, plafond_used = 1700 EUR
--   1700 / 2000 = 85% > 80% → I-16 scatta
--
-- Nota: I-16 viene valutato ad ogni transazione (qualsiasi).
--       Non serve una transazione specifica — basta che il CardProfileState
--       sia popolato correttamente via CDC dalla tabella cards.
--
-- Sequenza:
--   1. Esegui 00_cleanup.sql
--   2. Esegui questo script
--   3. Aspetta che Debezium propagi il CDC di cards → Flink aggiorna CardProfileState
--   4. Inserisci qualsiasi transazione → verifica events.enriched
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
    ('f0000016-0000-0000-0000-000000000016',
     'Giovanni', 'Marini', '1980-03-22', 'Napoli', 'MRNGNN80C22F839X',
     'retail', 'ordinary', NULL, 'ordinary',
     'medium', 58.0, '2017-07-01', true);

-- ============================================================
-- ACCOUNT
-- ============================================================
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'checking', 'IT12A0000000016000000000016', 'EUR',
     3000.00, '2017-07-01', 'active', 500.00);

-- ============================================================
-- CARD — credito con plafond quasi esaurito
--
-- plafond_limit = 2000 EUR
-- plafond_used  = 1700 EUR  → 85% > 80% → I-16 scatta
--
-- Debezium cattura questo INSERT → CardEvent →
-- CustomerProfileFunction.processElement2 CARD →
-- CardProfileState aggiornato in RocksDB
-- ============================================================
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f1000016-0000-0000-0000-000000000016',
     'credit', '4111111111110016',
     2000.00, 1700.00, 15, 'active',
     '2017-07-01', '2028-12-31');

-- ============================================================
-- CRM_PROFILE
-- ============================================================
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'retail', '["checking","credit_card"]', false, false,
     58.0, 0.20, 'app', true,
     140, 1, 15, 0.52);

-- ============================================================
-- MARKET DATA
-- ============================================================
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000016-0000-0000-0000-000000000016',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- ============================================================
-- TRANSAZIONI STORICHE — cold start protection
--
-- Servono >= 5 transazioni per uscire dal cold start.
-- Usiamo grocery/pos con card_id — ordinarie, non triggherano altri insight.
-- ============================================================
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f2000016-0000-0000-0000-000000000016',
     -50.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '25 days', CURRENT_DATE - 25, false, NULL),

    (gen_random_uuid(),
     'f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f2000016-0000-0000-0000-000000000016',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, NULL),

    (gen_random_uuid(),
     'f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f2000016-0000-0000-0000-000000000016',
     -45.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '15 days', CURRENT_DATE - 15, false, NULL),

    (gen_random_uuid(),
     'f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f2000016-0000-0000-0000-000000000016',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, NULL),

    (gen_random_uuid(),
     'f1000016-0000-0000-0000-000000000016',
     'f0000016-0000-0000-0000-000000000016',
     'f2000016-0000-0000-0000-000000000016',
     -40.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '5 days', CURRENT_DATE - 5, false, NULL);

-- ============================================================
-- TRANSAZIONE TRIGGER — qualsiasi, serve solo per far girare il CEP
--
-- Inserisci a NOW() dopo ~30 secondi (tempo per CDC cards → Flink).
-- Il CEP valuta I-16 su ogni TRANSACTION — non dipende dall'importo.
-- ============================================================
-- Esegui separatamente dopo aver verificato che il CardProfileState
-- sia stato aggiornato (vedi log Flink o attendi ~30s dal cleanup):
--
-- INSERT INTO smash_own.transactions
--     (transaction_id, account_id, customer_id, card_id,
--      amount, currency, merchant_category, channel, counterpart,
--      transaction_date, value_date, is_recurring, pattern_phase)
-- VALUES
--     (gen_random_uuid(),
--      'f1000016-0000-0000-0000-000000000016',
--      'f0000016-0000-0000-0000-000000000016',
--      'f2000016-0000-0000-0000-000000000016',
--      -30.00, 'EUR', 'grocery', 'pos', NULL,
--      NOW(), CURRENT_DATE, false, 'i16_trigger');

-- ============================================================
-- VERIFICA ATTESA
--
-- CardProfileState dopo CDC cards:
--   cardType    = credit ✅
--   status      = active ✅
--   plafondUsed = 1700 EUR
--   plafondLimit= 2000 EUR
--   usagePct    = 85% > 80% ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000016-0000-0000-0000-000000000016
--   detectedPatterns: ["I-16:f2000016-0000-0000-0000-000000000016"]
-- ============================================================
SELECT
    'Setup I-16 completato' AS status,
    'f0000016-0000-0000-0000-000000000016' AS customer_id,
    'f2000016-0000-0000-0000-000000000016' AS card_id,
    '85% plafond usato (1700/2000)' AS logica,
    'Attendi ~30s CDC cards → Flink, poi esegui INSERT trigger commentato' AS prossimo_passo;