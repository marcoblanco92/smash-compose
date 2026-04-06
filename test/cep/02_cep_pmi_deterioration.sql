-- ============================================================
-- 02_cep_pmi_deterioration.sql
-- Pattern: P-02-PHASE1 + P-02-PHASE2
--
-- UUID fissi:
--   CUSTOMER A : 10000002-0000-0000-0000-000000000001
--   ACCOUNT  A : 20000002-0000-0000-0000-000000000001
--   LOAN     A : 40000002-0000-0000-0000-000000000001
--   CARD     A : 50000002-0000-0000-0000-000000000001
--   CUSTOMER B : 10000002-0000-0000-0000-000000000002
--   ACCOUNT  B : 20000002-0000-0000-0000-000000000002
--   LOAN     B : 40000002-0000-0000-0000-000000000002
--   CARD     B : 50000002-0000-0000-0000-000000000002
--
-- ✅ Cliente A: NESSUNA dipendenza smash-batch (loan CDC diretto).
-- ⚠️  Cliente B: DIPENDENZA smash-batch — storico ordinario aggiunto
--     per superare COLD_START_THRESHOLD=5. Attendere ~5 min.
--
-- Output atteso:
--   Cliente A → detectedPatterns: ["P-02-PHASE1"]
--   Cliente B → detectedPatterns: ["P-02-PHASE2"]
-- ============================================================

SET search_path TO smash_own;

-- ══════════════════════════════════════════════════════════
-- CLIENTE A — P-02-PHASE1 (stress silenzioso, DPD=0)
-- ══════════════════════════════════════════════════════════

INSERT INTO smash_own.customers (
    customer_id, first_name, last_name, birth_date, birth_place, tax_code,
    segment, pattern_type, pattern_trigger_date, active_pattern,
    risk_class, clv_score, relationship_mgr, onboarding_date, is_active
) VALUES (
             '10000002-0000-0000-0000-000000000001'::uuid,
             'Giuseppe', 'Verdi', '1972-06-10'::date, 'Torino', 'VRDGPP72H10L219B',
             'pmi', 'pmi_deterioration', CURRENT_DATE, 'pmi_deterioration',
             'medium', 65.00,
             '99000002-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '7 years', TRUE
         );

INSERT INTO smash_own.accounts (
    account_id, customer_id, account_type, iban, currency,
    current_balance, opened_date, status, overdraft_limit, updated_at
) VALUES (
             '20000002-0000-0000-0000-000000000001'::uuid,
             '10000002-0000-0000-0000-000000000001'::uuid,
             'business', 'IT60X0542811101000000123402', 'EUR',
             8500.00, CURRENT_DATE - INTERVAL '7 years', 'active', 50000.00, NOW()
         );

-- Carta business debit
INSERT INTO smash_own.cards (
    card_id, customer_id, account_id, card_type, card_number,
    plafond_limit, plafond_used, billing_cycle_day,
    status, issued_date, expiry_date, updated_at
) VALUES (
             '50000002-0000-0000-0000-000000000001'::uuid,
             '10000002-0000-0000-0000-000000000001'::uuid,
             '20000002-0000-0000-0000-000000000001'::uuid,
             'debit', '4222222222220001',
             NULL, 0.00, NULL,
             'active', CURRENT_DATE - INTERVAL '7 years',
             CURRENT_DATE + INTERVAL '1 year', NOW()
         );

-- credit_line_usage_pct=78 → ②   days_past_due=0 → ③ PHASE1
INSERT INTO smash_own.loans (
    loan_id, customer_id, loan_type, principal_amount,
    outstanding_balance, interest_rate, start_date,
    maturity_date, next_due_date, days_past_due,
    credit_line_usage_pct, avg_payment_delay_days,
    status, collateral_type, updated_at
) VALUES (
             '40000002-0000-0000-0000-000000000001'::uuid,
             '10000002-0000-0000-0000-000000000001'::uuid,
             'credit_line', 250000.00, 195000.00, 5.250,
             CURRENT_DATE - INTERVAL '3 years',
             CURRENT_DATE + INTERVAL '2 years',
             CURRENT_DATE + INTERVAL '28 days',
             0,      -- ③ DPD=0 → PHASE1
             78.00,  -- ② > 70%
             8,
             'active', 'none', NOW()
         );

INSERT INTO smash_own.crm_profiles (
    profile_id, customer_id, segment, products_held,
    has_mortgage, has_investments, clv_score, churn_risk_score,
    relationship_mgr, last_contact_date, preferred_channel,
    push_opt_in, avg_session_duration_30d, push_ignore_streak,
    days_since_last_contact, product_usage_score, updated_at
) VALUES (
             '30000002-0000-0000-0000-000000000001'::uuid,
             '10000002-0000-0000-0000-000000000001'::uuid,
             'pmi', '["business_account","credit_line"]'::jsonb,
             FALSE, FALSE, 65.00, 0.250,
             '99000002-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '30 days',
             'branch', TRUE, 180, 0, 30, 0.500, NOW()
         );

INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -12500.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
       NOW() - INTERVAL '85 days', (NOW() - INTERVAL '85 days')::date,
       NULL, FALSE, 'pmi_silent_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -8900.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
       NOW() - INTERVAL '82 days', (NOW() - INTERVAL '82 days')::date,
       NULL, FALSE, 'pmi_silent_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -6200.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER00VERDE0003', NULL,
       NOW() - INTERVAL '78 days', (NOW() - INTERVAL '78 days')::date,
       NULL, FALSE, 'pmi_silent_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -11800.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
       NOW() - INTERVAL '55 days', (NOW() - INTERVAL '55 days')::date,
       NULL, FALSE, 'pmi_silent_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -7600.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
       NOW() - INTERVAL '50 days', (NOW() - INTERVAL '50 days')::date,
       NULL, FALSE, 'pmi_silent_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -9200.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
       NOW() - INTERVAL '20 days', (NOW() - INTERVAL '20 days')::date,
       NULL, FALSE, 'pmi_silent_stress');

-- Trigger A → P-02-PHASE1
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000001'::uuid,
       '10000002-0000-0000-0000-000000000001'::uuid,
       -5500.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
       NOW(), CURRENT_DATE, NULL, FALSE, 'pmi_silent_stress');


-- ══════════════════════════════════════════════════════════
-- CLIENTE B — P-02-PHASE2 (stress esplicito, DPD>0)
-- ══════════════════════════════════════════════════════════

INSERT INTO smash_own.customers (
    customer_id, first_name, last_name, birth_date, birth_place, tax_code,
    segment, pattern_type, pattern_trigger_date, active_pattern,
    risk_class, clv_score, relationship_mgr, onboarding_date, is_active
) VALUES (
             '10000002-0000-0000-0000-000000000002'::uuid,
             'Anna', 'Bianchi', '1968-11-22'::date, 'Roma', 'BNCNNA68S62H501C',
             'pmi', 'pmi_deterioration', CURRENT_DATE, 'pmi_deterioration',
             'high', 45.00,
             '99000002-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '5 years', TRUE
         );

INSERT INTO smash_own.accounts (
    account_id, customer_id, account_type, iban, currency,
    current_balance, opened_date, status, overdraft_limit, updated_at
) VALUES (
             '20000002-0000-0000-0000-000000000002'::uuid,
             '10000002-0000-0000-0000-000000000002'::uuid,
             'business', 'IT60X0542811101000000123403', 'EUR',
             2100.00, CURRENT_DATE - INTERVAL '5 years', 'active', 80000.00, NOW()
         );

INSERT INTO smash_own.cards (
    card_id, customer_id, account_id, card_type, card_number,
    plafond_limit, plafond_used, billing_cycle_day,
    status, issued_date, expiry_date, updated_at
) VALUES (
             '50000002-0000-0000-0000-000000000002'::uuid,
             '10000002-0000-0000-0000-000000000002'::uuid,
             '20000002-0000-0000-0000-000000000002'::uuid,
             'debit', '4222222222220002',
             NULL, 0.00, NULL,
             'active', CURRENT_DATE - INTERVAL '5 years',
             CURRENT_DATE + INTERVAL '1 year', NOW()
         );

-- credit_line_usage_pct=82 → ②   days_past_due=3 → ④ PHASE2
INSERT INTO smash_own.loans (
    loan_id, customer_id, loan_type, principal_amount,
    outstanding_balance, interest_rate, start_date,
    maturity_date, next_due_date, days_past_due,
    credit_line_usage_pct, avg_payment_delay_days,
    status, collateral_type, updated_at
) VALUES (
             '40000002-0000-0000-0000-000000000002'::uuid,
             '10000002-0000-0000-0000-000000000002'::uuid,
             'credit_line', 200000.00, 164000.00, 5.750,
             CURRENT_DATE - INTERVAL '4 years',
             CURRENT_DATE + INTERVAL '1 year',
             CURRENT_DATE - INTERVAL '3 days',
             3,      -- ④ DPD=3 → PHASE2
             82.00,  -- ② > 70%
             14,
             'active', 'none', NOW()
         );

INSERT INTO smash_own.crm_profiles (
    profile_id, customer_id, segment, products_held,
    has_mortgage, has_investments, clv_score, churn_risk_score,
    relationship_mgr, last_contact_date, preferred_channel,
    push_opt_in, avg_session_duration_30d, push_ignore_streak,
    days_since_last_contact, product_usage_score, updated_at
) VALUES (
             '30000002-0000-0000-0000-000000000002'::uuid,
             '10000002-0000-0000-0000-000000000002'::uuid,
             'pmi', '["business_account","credit_line"]'::jsonb,
             FALSE, FALSE, 45.00, 0.620,
             '99000002-0000-0000-0000-000000000001'::uuid,
             CURRENT_DATE - INTERVAL '60 days',
             'branch', TRUE, 120, 2, 60, 0.300, NOW()
         );

-- Storico ordinario mesi -6/-5/-4/-3 (baseline per smash-batch)
-- Serve a superare COLD_START_THRESHOLD=5 → isColdStart=false → CEP valutato
-- PMI sana: 3-4 fornitori/settimana, nessun salary_advance, nessun ritardo
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
-- m-6
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -18000.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
 NOW() - INTERVAL '175 days', (NOW() - INTERVAL '175 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -12000.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
 NOW() - INTERVAL '172 days', (NOW() - INTERVAL '172 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -9500.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER00VERDE0003', NULL,
 NOW() - INTERVAL '168 days', (NOW() - INTERVAL '168 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
-- m-5
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -17500.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
 NOW() - INTERVAL '145 days', (NOW() - INTERVAL '145 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -11000.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
 NOW() - INTERVAL '141 days', (NOW() - INTERVAL '141 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
-- m-4
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -16800.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
 NOW() - INTERVAL '115 days', (NOW() - INTERVAL '115 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -10500.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
 NOW() - INTERVAL '112 days', (NOW() - INTERVAL '112 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -8200.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER00VERDE0003', NULL,
 NOW() - INTERVAL '108 days', (NOW() - INTERVAL '108 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
-- m-3
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -15900.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
 NOW() - INTERVAL '85 days', (NOW() - INTERVAL '85 days')::date,
 NULL, FALSE, 'ordinary_baseline'),
(gen_random_uuid(),
 '20000002-0000-0000-0000-000000000002'::uuid,
 '10000002-0000-0000-0000-000000000002'::uuid,
 -9800.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0BIANCHI002', NULL,
 NOW() - INTERVAL '81 days', (NOW() - INTERVAL '81 days')::date,
 NULL, FALSE, 'ordinary_baseline');

-- Transazioni stress esplicito (fase deterioramento)
INSERT INTO smash_own.transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, card_id,
    transaction_date, value_date, description, is_recurring, pattern_phase
) VALUES
      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000002'::uuid,
       '10000002-0000-0000-0000-000000000002'::uuid,
       -4500.00, 'EUR', 'salary_advance', 'wire', 'IT00SALARYADVANCEPROV01', NULL,
       NOW() - INTERVAL '35 days', (NOW() - INTERVAL '35 days')::date,
       NULL, FALSE, 'pmi_explicit_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000002'::uuid,
       '10000002-0000-0000-0000-000000000002'::uuid,
       -3200.00, 'EUR', 'tax_payment', 'wire', 'IT00AGENZIAENTRATEF2401', NULL,
       NOW() - INTERVAL '18 days', (NOW() - INTERVAL '18 days')::date,
       NULL, FALSE, 'pmi_explicit_stress'),

      (gen_random_uuid(),
       '20000002-0000-0000-0000-000000000002'::uuid,
       '10000002-0000-0000-0000-000000000002'::uuid,
       -6800.00, 'EUR', 'b2b_transfer', 'wire', 'IT00SUPPLIER0ROSSI00001', NULL,
       NOW() - INTERVAL '10 days', (NOW() - INTERVAL '10 days')::date,
       NULL, FALSE, 'pmi_explicit_stress');

-- Trigger B → P-02-PHASE2
-- INSERT INTO smash_own.transactions (
--     transaction_id, account_id, customer_id, amount, currency,
--     merchant_category, channel, counterpart, card_id,
--     transaction_date, value_date, description, is_recurring, pattern_phase
-- ) VALUES (gen_random_uuid(),
--        '20000002-0000-0000-0000-000000000002'::uuid,
--        '10000002-0000-0000-0000-000000000002'::uuid,
--        -2100.00, 'EUR', 'tax_payment', 'wire', 'IT00AGENZIAENTRATEF2401', NULL,
--        NOW(), CURRENT_DATE, NULL, FALSE, 'pmi_explicit_stress');

-- ── MARKET DATA ───────────────────────────────────────────
-- Contesto P-02: spread BTP-Bund in salita → stress PMI amplificato
INSERT INTO smash_own.market_data (
    record_id, data_type, metric_name, value, previous_value,
    recorded_at, source
) VALUES
      (gen_random_uuid(), 'index', 'btp_bund_spread',
       185.50000, 172.30000, NOW() - INTERVAL '1 day', 'synthetic'),

      (gen_random_uuid(), 'ecb_rate', 'ecb_deposit_rate',
       3.25000, 3.50000, NOW() - INTERVAL '2 days', 'synthetic');

-- ── VERIFICA ──────────────────────────────────────────────
SELECT c.customer_id, c.segment::text, l.credit_line_usage_pct,
       l.days_past_due, l.avg_payment_delay_days,
       CASE
           WHEN l.days_past_due = 0 AND l.credit_line_usage_pct >= 70 THEN 'ATTESO: P-02-PHASE1'
           WHEN l.days_past_due > 0 AND l.credit_line_usage_pct >= 70 THEN 'ATTESO: P-02-PHASE2'
           ELSE 'NESSUN PATTERN'
           END AS pattern_atteso
FROM smash_own.customers c
         JOIN smash_own.loans l USING (customer_id)
WHERE c.customer_id IN (
                        '10000002-0000-0000-0000-000000000001'::uuid,
                        '10000002-0000-0000-0000-000000000002'::uuid
    )
ORDER BY c.customer_id;