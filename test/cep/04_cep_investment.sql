-- ============================================================
-- 04_cep_investment.sql
-- Pattern: P-04 — Opportunità Investimento
--
-- Logica CEP v3: trend di accumulo su w90 (3 mesi)
--   slope OLS >= estimatedMonthlyIncome * 8%
--   max 1 deviazione su 3 bucket, mai 2 consecutive
--
-- Profilo realistico:
--   Cliente affluent che negli ultimi 3 mesi accumula liquidità
--   senza investirla. Storico ordinario (spende quasi tutto) nei
--   mesi precedenti. Trend crescente netto negli ultimi 3 mesi.
--
-- Storico costruito:
--   m-2 (90-60gg fa):  net +800  (primo mese accumulo)
--   m-1 (60-30gg fa):  net +2200 (accumulo accelera)
--   m0  (ultimi 30gg): net +3100 (mese corrente — forte accumulo)
--
-- Slope OLS su [800, 2200, 3100] ≈ +1150/mese
-- estimatedMonthlyIncome = 5500 → soglia = 5500*0.08 = 440
-- 1150 > 440 ✓
-- Deviazioni (< 1150*0.70=805): m-2(800<805) = 1 dev ≤ 1 ✓
-- Non consecutive (solo 1) ✓ → P-04 scatta ✓
--
-- ⚠️  DIPENDENZA smash-batch: attendere ~5 min dopo l'esecuzione.
-- Output atteso: detectedPatterns: ["P-04"]
-- ============================================================

SET search_path TO smash_own;

-- ── 1. CUSTOMER ───────────────────────────────────────────
INSERT INTO smash_own.customers (
    customer_id, first_name, last_name, birth_date, birth_place, tax_code,
    segment, pattern_type, pattern_trigger_date, active_pattern,
    risk_class, clv_score, relationship_mgr, onboarding_date, is_active
) VALUES (
             '10000004-0000-0000-0000-000000000001'::uuid,
             'Sofia', 'Ricci', '1985-02-14'::date, 'Firenze', 'RCCSFO85B54D612P',
             'affluent', 'investment_opportunity', CURRENT_DATE,
             'ordinary',
             'low', 88.50,
             '99000004-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '6 years', TRUE
         );

-- ── 2. ACCOUNT ────────────────────────────────────────────
-- Saldo coerente con 3 mesi di accumulo: 800+2200+3100 = 6100
INSERT INTO smash_own.accounts (
    account_id, customer_id, account_type, iban, currency,
    current_balance, opened_date, status, overdraft_limit, updated_at
) VALUES (
             '20000004-0000-0000-0000-000000000001'::uuid,
             '10000004-0000-0000-0000-000000000001'::uuid,
             'checking', 'IT60X0542811101000000123405', 'EUR',
             6800.00,
             CURRENT_DATE - INTERVAL '6 years', 'active', 0.00, NOW()
         );

-- ── 3. CARDS ─────────────────────────────────────────────
INSERT INTO smash_own.cards (
    card_id, customer_id, account_id, card_type, card_number,
    plafond_limit, plafond_used, billing_cycle_day,
    status, issued_date, expiry_date, updated_at
) VALUES
      (
          '50000004-0000-0000-0000-000000000001'::uuid,
          '10000004-0000-0000-0000-000000000001'::uuid,
          '20000004-0000-0000-0000-000000000001'::uuid,
          'debit', '4444444444440001', NULL, 0.00, NULL,
          'active', CURRENT_DATE - INTERVAL '6 years', CURRENT_DATE + INTERVAL '2 years', NOW()
      ),
      (
          '50000004-0000-0000-0000-000000000002'::uuid,
          '10000004-0000-0000-0000-000000000001'::uuid,
          '20000004-0000-0000-0000-000000000001'::uuid,
          'credit', '4444444444440002', 5000.00, 600.00, 10,
          'active', CURRENT_DATE - INTERVAL '5 years', CURRENT_DATE + INTERVAL '1 year', NOW()
      );

-- ── 4. LOAN ──────────────────────────────────────────────
INSERT INTO smash_own.loans (
    loan_id, customer_id, loan_type, principal_amount,
    outstanding_balance, interest_rate, start_date,
    maturity_date, next_due_date, days_past_due,
    credit_line_usage_pct, avg_payment_delay_days,
    status, collateral_type, updated_at
) VALUES (
             '40000004-0000-0000-0000-000000000001'::uuid,
             '10000004-0000-0000-0000-000000000001'::uuid,
             'personal', 15000.00, 0.00, 5.500,
             CURRENT_DATE - INTERVAL '4 years',
             CURRENT_DATE - INTERVAL '6 months',
             NULL, 0, NULL, 0,
             'closed', 'none', NOW()
         );

-- ── 5. CRM PROFILE ───────────────────────────────────────
INSERT INTO smash_own.crm_profiles (
    profile_id, customer_id, segment, products_held,
    has_mortgage, has_investments, clv_score, churn_risk_score,
    relationship_mgr, last_contact_date, preferred_channel,
    push_opt_in, avg_session_duration_30d, push_ignore_streak,
    days_since_last_contact, product_usage_score, updated_at
) VALUES (
             '30000004-0000-0000-0000-000000000001'::uuid,
             '10000004-0000-0000-0000-000000000001'::uuid,
             'affluent', '["checking","savings"]'::jsonb,
             FALSE, FALSE,
             88.50, 0.080,
             '99000004-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '20 days',
             'app', TRUE, 280, 0, 20, 0.700, NOW()
         );

-- ── 6. TRANSAZIONI STORICHE ───────────────────────────────
-- Mesi precedenti m-2 (storico ordinario — cliente spende molto)
-- Serve per popolare w365 e dare contesto storico a smash-batch
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
-- m-5/-4/-3: ordinario (spende quasi tutto)
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '165 days', (NOW() - INTERVAL '165 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -5200.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '163 days', (NOW() - INTERVAL '163 days')::date, NULL, TRUE, 'ordinary_baseline'),

(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '135 days', (NOW() - INTERVAL '135 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -5150.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '133 days', (NOW() - INTERVAL '133 days')::date, NULL, TRUE, 'ordinary_baseline'),

(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '105 days', (NOW() - INTERVAL '105 days')::date, NULL, TRUE, 'ordinary_baseline'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -5100.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '103 days', (NOW() - INTERVAL '103 days')::date, NULL, TRUE, 'ordinary_baseline'),

-- ── m-2: net +800 (inizio accumulo) ──────────────────────
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '75 days', (NOW() - INTERVAL '75 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -3200.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '73 days', (NOW() - INTERVAL '73 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -900.00, 'EUR', 'grocery', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '68 days', (NOW() - INTERVAL '68 days')::date, NULL, FALSE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -600.00, 'EUR', 'restaurant_cafe', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '63 days', (NOW() - INTERVAL '63 days')::date, NULL, FALSE, 'investment_accumulation'),

-- ── m-1: net +2200 (accumulo accelera) ────────────────────
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '45 days', (NOW() - INTERVAL '45 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -1900.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '43 days', (NOW() - INTERVAL '43 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -900.00, 'EUR', 'grocery', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '38 days', (NOW() - INTERVAL '38 days')::date, NULL, FALSE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -400.00, 'EUR', 'restaurant_cafe', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '33 days', (NOW() - INTERVAL '33 days')::date, NULL, FALSE, 'investment_accumulation'),

-- ── m0: net +3100 (mese corrente — forte accumulo) ─────────
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 5500.00, 'EUR', 'salary_income', 'wire', 'IT00EMPLOYER0AFFLUENT01', NULL,
 NOW() - INTERVAL '15 days', (NOW() - INTERVAL '15 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -1500.00, 'EUR', 'utilities', 'sepa_dd', 'IT00UTILITY000000000001', NULL,
 NOW() - INTERVAL '13 days', (NOW() - INTERVAL '13 days')::date, NULL, TRUE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -600.00, 'EUR', 'grocery', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '8 days', (NOW() - INTERVAL '8 days')::date, NULL, FALSE, 'investment_accumulation'),
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
 -300.00, 'EUR', 'restaurant_cafe', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
 NOW() - INTERVAL '4 days', (NOW() - INTERVAL '4 days')::date, NULL, FALSE, 'investment_accumulation');

-- ── 7. MARKET DATA ────────────────────────────────────────
INSERT INTO smash_own.market_data (
    record_id, data_type, metric_name, value, previous_value, recorded_at, source
) VALUES
      (gen_random_uuid(), 'ecb_rate', 'ecb_deposit_rate', 3.00000, 3.25000, NOW() - INTERVAL '1 day', 'synthetic'),
      (gen_random_uuid(), 'irs_curve', 'irs_10y', 3.15000, 3.41000, NOW() - INTERVAL '1 day', 'synthetic'),
      (gen_random_uuid(), 'index', 'ftse_mib', 34200.00000, 33800.00000, NOW() - INTERVAL '1 day', 'synthetic');

-- ── 8. APP EVENTS — SEGNALI INVESTIMENTO ─────────────────
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES
      (gen_random_uuid(), '10000004-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'investimenti/fondi', gen_random_uuid(), 240, NOW() - INTERVAL '4 days',
       'ios', FALSE, 'commercial', 5, FALSE),
      (gen_random_uuid(), '10000004-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'investimenti/simulatore', gen_random_uuid(), 195, NOW() - INTERVAL '3 days',
       'ios', FALSE, 'commercial', 4, FALSE),
      (gen_random_uuid(), '10000004-0000-0000-0000-000000000001'::uuid,
       'screen_view', 'investimenti/fondi', gen_random_uuid(), 280, NOW() - INTERVAL '1 day',
       'ios', FALSE, 'commercial', 6, TRUE);

-- ── 9. EVENTO TRIGGER ─────────────────────────────────────
INSERT INTO smash_own.app_events (
    event_id, customer_id, event_type, screen_name,
    session_id, session_duration_s, event_timestamp,
    device_type, is_push_opened, feature_category,
    screens_visited_n, is_return_visit
) VALUES (
             gen_random_uuid(), '10000004-0000-0000-0000-000000000001'::uuid,
             'screen_view', 'dashboard/home', gen_random_uuid(), 110, NOW(),
             'ios', FALSE, 'essential', 2, FALSE
         );


-- Trigger smash-batch
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
(gen_random_uuid(), '20000004-0000-0000-0000-000000000001'::uuid, '10000004-0000-0000-0000-000000000001'::uuid,
    -0.10, 'EUR', 'grocery', 'pos', NULL, '50000004-0000-0000-0000-000000000001'::uuid,
    NOW(), NOW()::date, NULL, FALSE, 'investment_accumulation');



-- ── 10. VERIFICA ──────────────────────────────────────────
SELECT
    date_trunc('month', transaction_date) AS mese,
    SUM(amount) AS net_flow,
    COUNT(*) AS n_txn
FROM smash_own.transactions
WHERE customer_id = '10000004-0000-0000-0000-000000000001'::uuid
GROUP BY 1 ORDER BY 1;

-- bucket w90 attesi da smash-batch: [800, 2200, 3100]
-- slope OLS ≈ 1150/mese > soglia 440 (5500*8%) ✓
-- deviazioni (< 805): m-2(800<805) = 1 <= 1 ✓ → P-04 scatta ✓

-- CHECKLIST:
-- 1. Attendere ~5 min → smash-batch → customer.baselines
-- 2. Kafka UI → customer.baselines → w90MonthlySums ≈ [800, 2200, 3100]?
-- 3. Kafka UI → events.enriched → detectedPatterns: ["P-04"]?
--    profileSnapshot.w90MonthlySlope > 440?
--    profileSnapshot.activePattern = "ordinary"?