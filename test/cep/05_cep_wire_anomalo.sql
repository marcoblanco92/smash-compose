-- ============================================================
-- 11_test_p05.sql — Test P-05: Wire Anomalo
--
-- STRUTTURA IN DUE PARTI:
--   PARTE A (questo script): setup base — esegui una volta sola
--   PARTE B (manuale):       sequenza real-time — vedi istruzioni in fondo
--
-- Pattern: wireCount30d >= 3
--          AND wireFromAppPct30d >= 80%
--          AND (txnTs - lastAppEventTs) > 30s sul trigger
--
-- Sequenza completa:
--   1. 00_cleanup.sql
--   2. Questo script (PARTE A)
--   3. Aspetta ~2 min → smash-batch risolve cold start
--   4. Esegui PARTE B manualmente (3 coppie app+wire + trigger)
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- PARTE A — SETUP BASE
-- ============================================================

-- CUSTOMER
INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES
    ('f0000005-0000-0000-0000-000000000005',
     'Marco', 'Digitale', '1987-05-18', 'Milano', 'DGTMRC87E18F205X',
     'retail', 'ordinary', NULL, 'ordinary',
     'low', 70.0, '2018-01-01', true);

-- ACCOUNT
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES
    ('f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'checking', 'IT12A0000000005000000000005', 'EUR',
     8000.00, '2018-01-01', 'active', 1000.00);

-- CARD
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status,
 issued_date, expiry_date)
VALUES
    ('f2000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f1000005-0000-0000-0000-000000000005',
     'debit', '4111111111110005',
     NULL, 0.00, 1, 'active',
     '2018-01-01', '2028-12-31');

-- CRM_PROFILE
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact,
 product_usage_score)
VALUES
    ('f3000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'retail', '["checking"]', false, false,
     70.0, 0.08, 'app', true,
     200, 0, 10, 0.75);

-- MARKET DATA
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value,
 recorded_at, source)
VALUES
    ('f4000005-0000-0000-0000-000000000005',
     'ecb_rate', 'ecb_rate', 4.50, 4.25,
     NOW() - INTERVAL '1 day', 'ECB');

-- TRANSAZIONI STORICHE — solo per cold start (>= 5 transazioni totali)
-- Tutte pos/grocery — non interferiscono con wireCount30d
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, card_id,
 amount, currency, merchant_category, channel, counterpart,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f2000005-0000-0000-0000-000000000005',
     -60.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '20 days', CURRENT_DATE - 20, false, NULL),

    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f2000005-0000-0000-0000-000000000005',
     -55.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '15 days', CURRENT_DATE - 15, false, NULL),

    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f2000005-0000-0000-0000-000000000005',
     -70.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '10 days', CURRENT_DATE - 10, false, NULL),

    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f2000005-0000-0000-0000-000000000005',
     -50.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '8 days', CURRENT_DATE - 8, false, NULL),

    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     'f2000005-0000-0000-0000-000000000005',
     -65.00, 'EUR', 'grocery', 'pos', NULL,
     NOW() - INTERVAL '5 days', CURRENT_DATE - 5, false, NULL);

SELECT
    'PARTE A completata' AS status,
    'Aspetta ~2 minuti, poi esegui PARTE B manualmente' AS prossimo_passo;


-- ============================================================
-- PARTE B — SEQUENZA REAL-TIME (esegui manualmente)
--
-- Copia ed esegui ogni blocco separatamente con il sleep indicato.
-- Lo sleep garantisce che Flink aggiorni lastAppEventTs prima del wire.
-- ============================================================

-- ── STEP 1: App event + Wire #1 ──────────────────────────────
-- Esegui app event:

INSERT INTO smash_own.app_events
    (event_id, customer_id, event_type, screen_name, session_id,
     session_duration_s, event_timestamp, device_type,
     is_push_opened, feature_category, screens_visited_n, is_return_visit)
VALUES
    (gen_random_uuid(),
     'f0000005-0000-0000-0000-000000000005',
     'screen_view', 'bonifici',
     gen_random_uuid(),
     120, NOW(), 'ios', false, 'essential', 3, false);


-- Aspetta 5 secondi, poi esegui wire #1:

INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     NULL,
     -500.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_alfa_srl',
     NOW(), CURRENT_DATE, false, 'p05_wire_from_app');


-- ── STEP 2: App event + Wire #2 (aspetta ~10s dal wire #1) ───

INSERT INTO smash_own.app_events
    (event_id, customer_id, event_type, screen_name, session_id,
     session_duration_s, event_timestamp, device_type,
     is_push_opened, feature_category, screens_visited_n, is_return_visit)
VALUES
    (gen_random_uuid(),
     'f0000005-0000-0000-0000-000000000005',
     'screen_view', 'bonifici',
     gen_random_uuid(),
     90, NOW(), 'ios', false, 'essential', 2, false);


-- Aspetta 5 secondi, poi esegui wire #2:

INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     NULL,
     -800.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_beta_srl',
     NOW(), CURRENT_DATE, false, 'p05_wire_from_app');


-- ── STEP 3: App event + Wire #3 (aspetta ~10s dal wire #2) ───

INSERT INTO smash_own.app_events
    (event_id, customer_id, event_type, screen_name, session_id,
     session_duration_s, event_timestamp, device_type,
     is_push_opened, feature_category, screens_visited_n, is_return_visit)
VALUES
    (gen_random_uuid(),
     'f0000005-0000-0000-0000-000000000005',
     'screen_view', 'bonifici',
     gen_random_uuid(),
     150, NOW(), 'ios', false, 'essential', 4, false);


-- Aspetta 5 secondi, poi esegui wire #3:

INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     NULL,
     -650.00, 'EUR', 'b2b_transfer', 'wire', 'fornitore_gamma_srl',
     NOW(), CURRENT_DATE, false, 'p05_wire_from_app');


-- Dopo step 3:
--   wireCount30d        = 3
--   wireFromAppCount30d = 3
--   wireFromAppPct30d   = 100%
--   lastAppEventTs      = ~5s fa

-- ── STEP 4: Aspetta >= 60 secondi senza toccare nulla ────────
-- Questo garantisce che lastAppEventTs sia > 30s prima del trigger.

-- ── STEP 5: Wire TRIGGER — nessun app event prima ────────────
-- wireCount30d = 4, wireFromAppPct = 3/4 = 75% — borderline.
-- Per sicurezza aggiungi un 4° wire-from-app prima del trigger:
-- ripeti il pattern app+wire una quarta volta, poi aspetta 60s,
-- poi esegui questo trigger → wireFromAppPct = 4/5 = 80% esatto.

INSERT INTO smash_own.transactions
    (transaction_id, account_id, customer_id, card_id,
     amount, currency, merchant_category, channel, counterpart,
     transaction_date, value_date, is_recurring, pattern_phase)
VALUES
    (gen_random_uuid(),
     'f1000005-0000-0000-0000-000000000005',
     'f0000005-0000-0000-0000-000000000005',
     NULL,
     -1200.00, 'EUR', 'b2b_transfer', 'wire', 'nuovo_fornitore_srl',
     NOW(), CURRENT_DATE, false, 'p05_trigger_anomalo');


-- ============================================================
-- VERIFICA ATTESA (dopo 4 wire-from-app + trigger)
--
--   wireCount30d        = 5 >= 3 ✅
--   wireFromAppCount30d = 4
--   wireFromAppPct30d   = 4/5 = 80% >= 80% ✅
--   (NOW() - lastAppEventTs) > 30s ✅
--   isWireWithoutDigitalOrigin = true ✅
--
-- PreEnrichedEvent atteso:
--   customerId      : f0000005-0000-0000-0000-000000000005
--   detectedPatterns: ["P-05"]
-- ============================================================