-- ============================================================
-- 01_cep_real_estate.sql
-- Pattern: P-01 — Real Estate Intent
--
-- Logica CEP v3: trend di accumulo su w180 (6 mesi)
--   slope OLS >= estimatedMonthlyIncome * 10%
--   max 3 deviazioni su 6 bucket, mai 2 consecutive
--
-- Profilo realistico:
--   Cliente retail che normalmente spende quasi tutto lo stipendio
--   (net flow mensile ~200-300€). Negli ultimi 3 mesi inizia ad
--   accumulare (~1500-2000€/mese netto). Trend monotono crescente
--   con una leggera flessione al mese -4 (evento straordinario).
--
-- Storico costruito:
--   m-5: net +250  (baseline ordinaria — quasi tutto speso)
--   m-4: net +150  (mese con spesa extra — leggera deviazione)
--   m-3: net +300  (ritorno alla norma)
--   m-2: net +1200 (inizio accumulo — uscite calano)
--   m-1: net +1800 (accumulo accelera)
--   m0:  net +2100 (mese corrente — accumulo forte)
--
-- Slope OLS su [250,150,300,1200,1800,2100] ≈ +380/mese
-- estimatedMonthlyIncome = 3000 → soglia = 300
-- 380 > 300 ✓
-- Deviazioni (< 380*0.70=266): m-5(250<266 ✓dev), m-4(150<266 ✓dev)
-- = 2 deviazioni su 6 (<=3 ✓), non consecutive (m-5 e m-4 sono consecutive!)
-- → Attenzione: m-5 e m-4 sono consecutive → FAIL con parametri attuali
--
-- Correzione: m-4 = 320 (sopra soglia)
--   Slope OLS su [250,320,300,1200,1800,2100] ≈ +375/mese
--   Deviazioni: solo m-5(250<262) = 1 deviazione → 1<=3 ✓, non consecutive ✓
--   → P-01 scatta ✓
--
-- ⚠️  DIPENDENZA smash-batch: attendere ~5 min dopo l'esecuzione.
-- Output atteso: detectedPatterns: ["P-01"]
-- ============================================================

SET search_path TO smash_own;

-- ── 1. CUSTOMER ───────────────────────────────────────────
INSERT INTO smash_own.customers (
    customer_id, first_name, last_name, birth_date, birth_place, tax_code,
    segment, pattern_type, pattern_trigger_date, active_pattern,
    risk_class, clv_score, relationship_mgr, onboarding_date, is_active
) VALUES (
             '10000001-0000-0000-0000-000000000001'::uuid,
             'Mario', 'Rossi', '1980-03-15'::date, 'Milano', 'RSSMRA80C15F205X',
             'retail', 'real_estate', CURRENT_DATE,
             'ordinary',  -- activePattern: verrà propagato da CustomerEvent CDC
             'medium', 72.50,
             '99000001-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '4 years', TRUE
         );

-- ── 2. ACCOUNT ────────────────────────────────────────────
-- current_balance: saldo attuale dopo 6 mesi di accumulo.
-- Flink usa questo come snapshot iniziale (bootstrap),
-- poi aggiorna currentBalance ad ogni transazione.
-- Valore: stipendio accumulato negli ultimi 6 mesi minus spese
-- ≈ 250+320+300+1200+1800+2100 = 5970€ accumulati in 6 mesi
INSERT INTO smash_own.accounts (
    account_id, customer_id, account_type, iban, currency,
    current_balance, opened_date, status, overdraft_limit, updated_at
) VALUES (
             '20000001-0000-0000-0000-000000000001'::uuid,
             '10000001-0000-0000-0000-000000000001'::uuid,
             'checking', 'IT60X0542811101000000123401', 'EUR',
             6200.00,  -- saldo attuale coerente con 6 mesi di accumulo progressivo
             CURRENT_DATE - INTERVAL '4 years', 'active', 0.00, NOW()
         );

-- ── 3. CARDS ─────────────────────────────────────────────
INSERT INTO smash_own.cards (
    card_id, customer_id, account_id, card_type, card_number,
    plafond_limit, plafond_used, billing_cycle_day,
    status, issued_date, expiry_date, updated_at
) VALUES
      (
          '50000001-0000-0000-0000-000000000001'::uuid,
          '10000001-0000-0000-0000-000000000001'::uuid,
          '20000001-0000-0000-0000-000000000001'::uuid,
          'debit', '4111111111110001', NULL, 0.00, NULL,
          'active', CURRENT_DATE - INTERVAL '4 years', CURRENT_DATE + INTERVAL '2 years', NOW()
      ),
      (
          '50000001-0000-0000-0000-000000000002'::uuid,
          '10000001-0000-0000-0000-000000000001'::uuid,
          '20000001-0000-0000-0000-000000000001'::uuid,
          'credit', '4111111111110002', 3000.00, 280.00, 15,
          'active', CURRENT_DATE - INTERVAL '3 years', CURRENT_DATE + INTERVAL '1 year', NOW()
      );

-- ── 4. LOAN ──────────────────────────────────────────────
INSERT INTO smash_own.loans (
    loan_id, customer_id, loan_type, principal_amount,
    outstanding_balance, interest_rate, start_date,
    maturity_date, next_due_date, days_past_due,
    credit_line_usage_pct, avg_payment_delay_days,
    status, collateral_type, updated_at
) VALUES (
             '40000001-0000-0000-0000-000000000001'::uuid,
             '10000001-0000-0000-0000-000000000001'::uuid,
             'personal', 8000.00, 3200.00, 6.500,
             CURRENT_DATE - INTERVAL '2 years',
             CURRENT_DATE + INTERVAL '1 year',
             CURRENT_DATE + INTERVAL '25 days',
             0, NULL, 0, 'active', 'none', NOW()
         );

-- ── 5. CRM PROFILE ───────────────────────────────────────
INSERT INTO smash_own.crm_profiles (
    profile_id, customer_id, segment, products_held,
    has_mortgage, has_investments, clv_score, churn_risk_score,
    relationship_mgr, last_contact_date, preferred_channel,
    push_opt_in, avg_session_duration_30d, push_ignore_streak,
    days_since_last_contact, product_usage_score, updated_at
) VALUES (
             '30000001-0000-0000-0000-000000000001'::uuid,
             '10000001-0000-0000-0000-000000000001'::uuid,
             'retail', '["checking"]'::jsonb,
             FALSE, FALSE, 72.50, 0.150,
             '99000001-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '45 days',
             'app', TRUE, 320, 1, 45, 0.600, NOW()
         );

-- ── 6. TRANSAZIONI STORICHE ───────────────────────────────
-- Costruiamo 6 mesi di storia con net flow crescente:
--
--   m-5 (180-150gg fa): net +250  → stipendio 3000, uscite 2750
--   m-4 (150-120gg fa): net +320  → stipendio 3000, uscite 2680
--   m-3 (120-90gg fa):  net +300  → stipendio 3000, uscite 2700
--   m-2 (90-60gg fa):   net +1200 → stipendio 3000, uscite 1800
--   m-1 (60-30gg fa):   net +1800 → stipendio 3000, uscite 1200
--   m0  (ultimi 30gg):  net +2100 → stipendio 3000, uscite 900
--
-- Slope OLS su [250,320,300,1200,1800,2100] ≈ +375/mese
-- Soglia slope: 3000 * 0.10 = 300 → 375 > 300 ✓
-- Deviazioni (< 375*0.70=262): solo m-5 (250<262) = 1 dev → ✓

INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
-- ── m-5: net +250 (baseline — quasi tutto speso) ──────────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '165 days', (NOW() - INTERVAL '165 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '163 days', (NOW() - INTERVAL '163 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -680.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '160 days', (NOW() - INTERVAL '160 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -420.00, 'EUR', 'restaurant_cafe', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '157 days', (NOW() - INTERVAL '157 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -800.00, 'EUR', 'transport', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '153 days', (NOW() - INTERVAL '153 days')::date, NULL, FALSE, 'ordinary_baseline'),

-- ── m-4: net +320 (mese normale) ──────────────────────────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '135 days', (NOW() - INTERVAL '135 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '133 days', (NOW() - INTERVAL '133 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -600.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '130 days', (NOW() - INTERVAL '130 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -430.00, 'EUR', 'ecommerce', 'online', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '126 days', (NOW() - INTERVAL '126 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -800.00, 'EUR', 'transport', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '122 days', (NOW() - INTERVAL '122 days')::date, NULL, FALSE, 'ordinary_baseline'),

-- ── m-3: net +300 (baseline ordinaria) ────────────────────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '105 days', (NOW() - INTERVAL '105 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '103 days', (NOW() - INTERVAL '103 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -650.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '100 days', (NOW() - INTERVAL '100 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -400.00, 'EUR', 'restaurant_cafe', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '96 days', (NOW() - INTERVAL '96 days')::date, NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -800.00, 'EUR', 'transport', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '92 days', (NOW() - INTERVAL '92 days')::date, NULL, FALSE, 'ordinary_baseline'),

-- ── m-2: net +1200 (inizio accumulo — uscite calano) ──────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '75 days', (NOW() - INTERVAL '75 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '73 days', (NOW() - INTERVAL '73 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -500.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '70 days', (NOW() - INTERVAL '70 days')::date, NULL, FALSE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -450.00, 'EUR', 'transport', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '65 days', (NOW() - INTERVAL '65 days')::date, NULL, FALSE, 'real_estate_intent'),

-- ── m-1: net +1800 (accumulo accelera) ────────────────────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '45 days', (NOW() - INTERVAL '45 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '43 days', (NOW() - INTERVAL '43 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -350.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '38 days', (NOW() - INTERVAL '38 days')::date, NULL, FALSE, 'real_estate_intent'),

-- ── m0: net +2100 (mese corrente) ─────────────────────────
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 3000.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0000000001', NULL,
 NOW() - INTERVAL '15 days', (NOW() - INTERVAL '15 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -850.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '13 days', (NOW() - INTERVAL '13 days')::date, NULL, TRUE, 'real_estate_intent'),
(gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
 -50.00, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '5 days', (NOW() - INTERVAL '5 days')::date, NULL, FALSE, 'real_estate_intent');

-- ── 7. MARKET DATA ────────────────────────────────────────
INSERT INTO smash_own.market_data (
    record_id, data_type, metric_name, value, previous_value, recorded_at, source
) VALUES
      (gen_random_uuid(), 'ecb_rate', 'ecb_deposit_rate', 3.25000, 3.50000, NOW() - INTERVAL '2 days', 'synthetic'),
      (gen_random_uuid(), 'irs_curve', 'irs_10y', 3.41000, 3.55000, NOW() - INTERVAL '1 day', 'synthetic');

-- ── 8. APP EVENTS — SEGNALI MUTUO ────────────────────────
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES
      (gen_random_uuid(), '10000001-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'mutui/simulazione', gen_random_uuid(), 185, NOW() - INTERVAL '5 days',
       'ios', FALSE, 'commercial', 4, FALSE),
      (gen_random_uuid(), '10000001-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'mutui/simulazione', gen_random_uuid(), 210, NOW() - INTERVAL '2 days',
       'ios', FALSE, 'commercial', 5, TRUE);

-- ── 9. EVENTO TRIGGER ─────────────────────────────────────
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES (
             gen_random_uuid(), '10000001-0000-0000-0000-000000000001'::uuid,
             'screen_view', 'dashboard/home', gen_random_uuid(), 95, NOW(),
             'ios', FALSE, 'essential', 2, FALSE
         );

-- ── 10. VERIFICA ──────────────────────────────────────────
SELECT 'CUSTOMER' AS entita, customer_id::text, segment::text, active_pattern::text
FROM smash_own.customers WHERE customer_id = '10000001-0000-0000-0000-000000000001'::uuid
UNION ALL
SELECT 'ACCOUNT', account_id::text, 'balance', current_balance::text
FROM smash_own.accounts WHERE customer_id = '10000001-0000-0000-0000-000000000001'::uuid
UNION ALL
SELECT 'CRM', customer_id::text, 'has_mortgage', has_mortgage::text
FROM smash_own.crm_profiles WHERE customer_id = '10000001-0000-0000-0000-000000000001'::uuid
UNION ALL
SELECT 'LOAN', loan_id::text, loan_type::text, days_past_due::text
FROM smash_own.loans WHERE customer_id = '10000001-0000-0000-0000-000000000001'::uuid;


-- Trigger smash-batch
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
    (gen_random_uuid(), '20000001-0000-0000-0000-000000000001'::uuid, '10000001-0000-0000-0000-000000000001'::uuid,
     -00.10, 'EUR', 'grocery', 'pos', NULL, '50000001-0000-0000-0000-000000000001'::uuid,
     NOW(), NOW()::date, NULL, FALSE, 'real_estate_intent');


-- Verifica net flow per mese (deve corrispondere allo schema sopra)
SELECT
    date_trunc('month', transaction_date) AS mese,
    SUM(amount) AS net_flow,
    COUNT(*) AS n_txn
FROM smash_own.transactions
WHERE customer_id = '10000001-0000-0000-0000-000000000001'::uuid
GROUP BY 1 ORDER BY 1;

-- Verifica slope attesa (calcolo manuale pre-Flink):
-- bucket mensili da smash-batch: [250, 320, 300, 1200, 1800, 2100]
-- slope OLS ≈ 375/mese > soglia 300 (3000*10%) ✓
-- deviazioni (< 262): solo m-5 = 1 <= 3 ✓, non consecutive ✓

-- CHECKLIST:
-- 1. Attendere ~5 min → smash-batch → customer.baselines
-- 2. Kafka UI → customer.baselines → w180MonthlySums presente?
-- 3. Kafka UI → events.enriched → detectedPatterns: ["P-01"]?
--    profileSnapshot.w180MonthlySlope > 300?
--    profileSnapshot.activePattern = "ordinary"?