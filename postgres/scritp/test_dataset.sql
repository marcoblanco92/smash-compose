-- ============================================================
-- test_dataset.sql — Dataset minimo per test e debug Flink
-- Real-Time Event Mesh con AI Enrichment
--
-- Contiene:
--   10 clienti (2 per pattern)
--   accounts, cards, crm_profiles, loans
--   transactions e app_events coerenti con il pattern
--   market_data (4 metriche)
--
-- Uso:
--   psql -h localhost -U smash_app -d smash -f test_dataset.sql
--
-- ATTENZIONE: trunca tutte le tabelle prima di inserire
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- CLEANUP
-- ============================================================
TRUNCATE smash_own.app_events      CASCADE;
TRUNCATE smash_own.transactions    CASCADE;
TRUNCATE smash_own.cards           CASCADE;
TRUNCATE smash_own.loans           CASCADE;
TRUNCATE smash_own.crm_profiles    CASCADE;
TRUNCATE smash_own.accounts        CASCADE;
TRUNCATE smash_own.market_data     CASCADE;
TRUNCATE smash_own.customers       CASCADE;


-- ============================================================
-- CUSTOMERS — 10 clienti, 2 per pattern
-- trigger_date = oggi - 4 settimane (siamo nel mezzo del pattern)
-- ============================================================

INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES
-- real_estate x2
('a0a0a0a0-0001-0001-0001-000000000001', 'Marco',    'Rossi',    '1985-03-15', 'Milano',  'RSSMRC85C15F205X', 'retail',   'real_estate', CURRENT_DATE - 28, 'real_estate',            'low',    72.50, '2018-06-01', true),
('a0a0a0a0-0002-0002-0002-000000000002', 'Giulia',   'Bianchi',  '1979-07-22', 'Roma',    'BNCGLI79L62H501Y', 'affluent', 'real_estate', CURRENT_DATE - 28, 'real_estate',            'low',    88.20, '2015-03-10', true),

-- pmi_deterioration x2
('a0a0a0a0-0003-0003-0003-000000000003', 'Roberto',  'Ferretti', '1972-11-08', 'Torino',  'FRRRBT72S08L219Z', 'pmi',      'pmi_deterioration', CURRENT_DATE - 28, 'pmi_deterioration', 'high',   45.00, '2016-09-15', true),
('a0a0a0a0-0004-0004-0004-000000000004', 'Lucia',    'Conti',    '1968-04-30', 'Napoli',  'CNTLCU68D70F839W', 'pmi',      'pmi_deterioration', CURRENT_DATE - 28, 'pmi_deterioration', 'high',   42.00, '2014-01-20', true),

-- pre_churn x2
('a0a0a0a0-0005-0005-0005-000000000005', 'Andrea',   'Marino',   '1990-09-12', 'Firenze', 'MRNNDR90P12D612V', 'retail',   'pre_churn', CURRENT_DATE - 28, 'pre_churn',              'medium', 55.00, '2020-02-14', true),
('a0a0a0a0-0006-0006-0006-000000000006', 'Chiara',   'Gallo',    '1988-01-25', 'Bologna', 'GLLCHR88A65A944U', 'retail',   'pre_churn', CURRENT_DATE - 28, 'pre_churn',              'medium', 51.00, '2019-07-01', true),

-- investment_opportunity x2
('a0a0a0a0-0007-0007-0007-000000000007', 'Stefano',  'Ricci',    '1982-06-18', 'Venezia', 'RCCSFN82H18L736T', 'affluent', 'investment_opportunity', CURRENT_DATE - 28, 'investment_opportunity', 'low', 91.00, '2013-11-05', true),
('a0a0a0a0-0008-0008-0008-000000000008', 'Francesca','Moretti',  '1975-12-03', 'Genova',  'MRTFNC75T43D969S', 'affluent', 'investment_opportunity', CURRENT_DATE - 28, 'investment_opportunity', 'low', 85.00, '2012-04-22', true),

-- ordinary x2
('a0a0a0a0-0009-0009-0009-000000000009', 'Luca',     'Bruno',    '1993-08-07', 'Palermo', 'BRNLCU93M07G273R', 'retail',   'ordinary', NULL, 'ordinary',                               'low',    48.00, '2021-05-10', true),
('a0a0a0a0-0010-0010-0010-000000000010', 'Elena',    'Costa',    '1987-02-14', 'Bari',    'CSTLNE87B54A662Q', 'retail',   'ordinary', NULL, 'ordinary',                               'low',    52.00, '2020-10-30', true);


-- ============================================================
-- ACCOUNTS — 1 per cliente (checking per retail/affluent, business per pmi)
-- ============================================================

INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency, current_balance, opened_date, status, overdraft_limit)
VALUES
-- real_estate: saldo alto (accumulo pre-acquisto)
('b0b0b0b0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', 'checking', 'IT12A0000000001000000000001', 'EUR', 32000.00, '2018-06-01', 'active', 500.00),
('b0b0b0b0-0002-0002-0002-000000000002', 'a0a0a0a0-0002-0002-0002-000000000002', 'checking', 'IT12A0000000002000000000002', 'EUR', 95000.00, '2015-03-10', 'active', 2000.00),

-- pmi_deterioration: saldo basso, linea di credito stressata
('b0b0b0b0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', 'business', 'IT12A0000000003000000000003', 'EUR', 8500.00,  '2016-09-15', 'active', 50000.00),
('b0b0b0b0-0004-0004-0004-000000000004', 'a0a0a0a0-0004-0004-0004-000000000004', 'business', 'IT12A0000000004000000000004', 'EUR', 6200.00,  '2014-01-20', 'active', 50000.00),

-- pre_churn: saldo in calo
('b0b0b0b0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005', 'checking', 'IT12A0000000005000000000005', 'EUR', 4200.00,  '2020-02-14', 'active', 0.00),
('b0b0b0b0-0006-0006-0006-000000000006', 'a0a0a0a0-0006-0006-0006-000000000006', 'checking', 'IT12A0000000006000000000006', 'EUR', 3800.00,  '2019-07-01', 'active', 0.00),

-- investment: saldo alto (liquidità in attesa)
('b0b0b0b0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', 'checking', 'IT12A0000000007000000000007', 'EUR', 185000.00,'2013-11-05', 'active', 2000.00),
('b0b0b0b0-0008-0008-0008-000000000008', 'a0a0a0a0-0008-0008-0008-000000000008', 'checking', 'IT12A0000000008000000000008', 'EUR', 142000.00,'2012-04-22', 'active', 2000.00),

-- ordinary: saldo normale
('b0b0b0b0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009', 'checking', 'IT12A0000000009000000000009', 'EUR', 12500.00, '2021-05-10', 'active', 0.00),
('b0b0b0b0-0010-0010-0010-000000000010', 'a0a0a0a0-0010-0010-0010-000000000010', 'checking', 'IT12A0000000010000000000010', 'EUR', 14800.00, '2020-10-30', 'active', 0.00);


-- ============================================================
-- CARDS — debit per tutti, credit per affluent e pmi
-- card_number in chiaro — mascherato da SMT Debezium
-- ============================================================

INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number, plafond_limit, plafond_used, billing_cycle_day, status, issued_date, expiry_date)
VALUES
-- real_estate
('c0c0c0c0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', 'b0b0b0b0-0001-0001-0001-000000000001', 'debit',  '4111111111110001', NULL,     0.00,    NULL, 'active', '2018-06-01', '2028-06-30'),
('c0c0c0c0-0002-0002-0002-000000000002', 'a0a0a0a0-0002-0002-0002-000000000002', 'b0b0b0b0-0002-0002-0002-000000000002', 'debit',  '4111111111110002', NULL,     0.00,    NULL, 'active', '2015-03-10', '2029-03-31'),
('c0c0c0c0-0002-0002-0002-000000000012', 'a0a0a0a0-0002-0002-0002-000000000002', 'b0b0b0b0-0002-0002-0002-000000000002', 'credit', '5411111111110002', 10000.00, 1200.00, 1,    'active', '2015-03-10', '2029-03-31'),

-- pmi_deterioration — credit con plafond stressato
('c0c0c0c0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', 'b0b0b0b0-0003-0003-0003-000000000003', 'debit',  '4111111111110003', NULL,     0.00,    NULL, 'active', '2016-09-15', '2026-09-30'),
('c0c0c0c0-0003-0003-0003-000000000013', 'a0a0a0a0-0003-0003-0003-000000000003', 'b0b0b0b0-0003-0003-0003-000000000003', 'credit', '5411111111110003', 25000.00, 21800.00, 15,  'active', '2016-09-15', '2026-09-30'),
('c0c0c0c0-0004-0004-0004-000000000004', 'a0a0a0a0-0004-0004-0004-000000000004', 'b0b0b0b0-0004-0004-0004-000000000004', 'debit',  '4111111111110004', NULL,     0.00,    NULL, 'active', '2014-01-20', '2027-01-31'),
('c0c0c0c0-0004-0004-0004-000000000014', 'a0a0a0a0-0004-0004-0004-000000000004', 'b0b0b0b0-0004-0004-0004-000000000004', 'credit', '5411111111110004', 20000.00, 18500.00, 15,  'active', '2014-01-20', '2027-01-31'),

-- pre_churn
('c0c0c0c0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005', 'b0b0b0b0-0005-0005-0005-000000000005', 'debit',  '4111111111110005', NULL,     0.00,    NULL, 'active', '2020-02-14', '2028-02-28'),
('c0c0c0c0-0006-0006-0006-000000000006', 'a0a0a0a0-0006-0006-0006-000000000006', 'b0b0b0b0-0006-0006-0006-000000000006', 'debit',  '4111111111110006', NULL,     0.00,    NULL, 'active', '2019-07-01', '2027-07-31'),

-- investment
('c0c0c0c0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', 'b0b0b0b0-0007-0007-0007-000000000007', 'debit',  '4111111111110007', NULL,     0.00,    NULL, 'active', '2013-11-05', '2027-11-30'),
('c0c0c0c0-0007-0007-0007-000000000017', 'a0a0a0a0-0007-0007-0007-000000000007', 'b0b0b0b0-0007-0007-0007-000000000007', 'credit', '5411111111110007', 15000.00, 2100.00, 1,    'active', '2013-11-05', '2027-11-30'),
('c0c0c0c0-0008-0008-0008-000000000008', 'a0a0a0a0-0008-0008-0008-000000000008', 'b0b0b0b0-0008-0008-0008-000000000008', 'debit',  '4111111111110008', NULL,     0.00,    NULL, 'active', '2012-04-22', '2026-04-30'),
('c0c0c0c0-0008-0008-0008-000000000018', 'a0a0a0a0-0008-0008-0008-000000000008', 'b0b0b0b0-0008-0008-0008-000000000008', 'credit', '5411111111110008', 20000.00, 3500.00, 1,    'active', '2012-04-22', '2026-04-30'),

-- ordinary
('c0c0c0c0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009', 'b0b0b0b0-0009-0009-0009-000000000009', 'debit',  '4111111111110009', NULL,     0.00,    NULL, 'active', '2021-05-10', '2029-05-31'),
('c0c0c0c0-0010-0010-0010-000000000010', 'a0a0a0a0-0010-0010-0010-000000000010', 'b0b0b0b0-0010-0010-0010-000000000010', 'debit',  '4111111111110010', NULL,     0.00,    NULL, 'active', '2020-10-30', '2028-10-31');


-- ============================================================
-- LOANS — solo pmi (credit_line stressata) e real_estate (no mortgage)
-- ============================================================

INSERT INTO smash_own.loans
(loan_id, customer_id, loan_type, principal_amount, outstanding_balance, interest_rate,
 start_date, maturity_date, next_due_date, days_past_due,
 credit_line_usage_pct, avg_payment_delay_days, status, collateral_type)
VALUES
-- pmi_deterioration: credit_line usage > 70% — segnale fase 1
('d0d0d0d0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', 'credit_line', 100000.00, 87000.00, 4.500, '2020-01-01', '2026-01-01', CURRENT_DATE + 15, 8,  87.00, 12, 'active', 'none'),
('d0d0d0d0-0004-0004-0004-000000000004', 'a0a0a0a0-0004-0004-0004-000000000004', 'credit_line', 80000.00,  71000.00, 4.750, '2019-06-01', '2025-06-01', CURRENT_DATE + 10, 5,  88.75, 15, 'active', 'none'),

-- real_estate: personal loan (no mortgage — has_mortgage=false è il segnale)
-- investment e ordinary: nessun loan
('d0d0d0d0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', 'personal', 15000.00, 8200.00, 6.200, '2021-03-01', '2026-03-01', CURRENT_DATE + 20, 0, NULL, 0, 'active', 'none');


-- ============================================================
-- CRM_PROFILES — 1 per cliente
-- ============================================================

INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage, has_investments,
 clv_score, churn_risk_score, preferred_channel, push_opt_in,
 avg_session_duration_30d, push_ignore_streak, days_since_last_contact, product_usage_score)
VALUES
-- real_estate: no mortgage, sessions ok, interesse mutui
('e0e0e0e0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', 'retail',   '["personal_loan"]',          false, false, 72.50, 0.120, 'app',    true,  185, 0, 45, 0.620),
('e0e0e0e0-0002-0002-0002-000000000002', 'a0a0a0a0-0002-0002-0002-000000000002', 'affluent', '["checking"]',               false, false, 88.20, 0.080, 'app',    true,  210, 1, 30, 0.580),

-- pmi_deterioration: score alto, sessions normali
('e0e0e0e0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', 'pmi',      '["credit_line","business"]', false, false, 45.00, 0.650, 'phone',  true,  95,  2, 22, 0.410),
('e0e0e0e0-0004-0004-0004-000000000004', 'a0a0a0a0-0004-0004-0004-000000000004', 'pmi',      '["credit_line","business"]', false, false, 42.00, 0.720, 'phone',  true,  88,  3, 18, 0.390),

-- pre_churn: session drop, push ignore alto, no contact recente
('e0e0e0e0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005', 'retail',   '["checking"]',               false, false, 55.00, 0.810, 'app',    true,  62,  5, 72, 0.220),
('e0e0e0e0-0006-0006-0006-000000000006', 'a0a0a0a0-0006-0006-0006-000000000006', 'retail',   '["checking"]',               false, false, 51.00, 0.790, 'app',    true,  58,  4, 68, 0.190),

-- investment: no investments, sessions attive
('e0e0e0e0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', 'affluent', '["checking"]',               false, false, 91.00, 0.050, 'app',    true,  240, 0, 12, 0.750),
('e0e0e0e0-0008-0008-0008-000000000008', 'a0a0a0a0-0008-0008-0008-000000000008', 'affluent', '["checking"]',               false, false, 85.00, 0.060, 'app',    true,  220, 0, 18, 0.700),

-- ordinary
('e0e0e0e0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009', 'retail',   '["checking"]',               false, false, 48.00, 0.150, 'app',    true,  142, 1, 20, 0.480),
('e0e0e0e0-0010-0010-0010-000000000010', 'a0a0a0a0-0010-0010-0010-000000000010', 'retail',   '["checking"]',               false, false, 52.00, 0.130, 'app',    true,  138, 0, 25, 0.510);


-- ============================================================
-- TRANSACTIONS — 3-5 per cliente, coerenti con il pattern
-- ============================================================

INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, card_id,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES

-- ── real_estate (Marco, cust 0001) — accumulo + prime spese agenzie ──
('f0f0f0f0-0001-0001-0001-000000000001', 'b0b0b0b0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001',  2800.00, 'EUR', 'salary_income',  'wire',   'IT99B0000099001000000000001', NULL,                                    NOW() - INTERVAL '10 days', CURRENT_DATE - 10, true,  'real_estate_intent'),
('f0f0f0f0-0001-0001-0001-000000000002', 'b0b0b0b0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001',  -350.00, 'EUR', 'real_estate',    'wire',   'IT99B0000099001000000000002', NULL,                                    NOW() - INTERVAL '7 days',  CURRENT_DATE - 7,  false, 'real_estate_search'),
('f0f0f0f0-0001-0001-0001-000000000003', 'b0b0b0b0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001',   -85.00, 'EUR', 'grocery',        'pos',    NULL,                         'c0c0c0c0-0001-0001-0001-000000000001',   NOW() - INTERVAL '3 days',  CURRENT_DATE - 3,  false, 'real_estate_intent'),
('f0f0f0f0-0001-0001-0001-000000000004', 'b0b0b0b0-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', -1200.00, 'EUR', 'legal_services', 'wire',   'IT99B0000099001000000000003', NULL,                                    NOW() - INTERVAL '1 day',   CURRENT_DATE - 1,  false, 'real_estate_formal'),

-- ── pmi_deterioration (Roberto, cust 0003) — b2b cala + salary_advance ──
('f0f0f0f0-0003-0003-0003-000000000001', 'b0b0b0b0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', 12000.00, 'EUR', 'b2b_transfer',   'wire',   'IT99B0000099003000000000001', NULL,                                    NOW() - INTERVAL '15 days', CURRENT_DATE - 15, false, 'pmi_silent_stress'),
('f0f0f0f0-0003-0003-0003-000000000002', 'b0b0b0b0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003',  2200.00, 'EUR', 'salary_advance', 'wire',   'IT99B0000099003000000000002', NULL,                                    NOW() - INTERVAL '8 days',  CURRENT_DATE - 8,  false, 'pmi_explicit_stress'),
('f0f0f0f0-0003-0003-0003-000000000003', 'b0b0b0b0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003', -4500.00, 'EUR', 'tax_payment',    'wire',   'IT99B0000099003000000000003', NULL,                                    NOW() - INTERVAL '5 days',  CURRENT_DATE - 5,  false, 'pmi_explicit_stress'),
('f0f0f0f0-0003-0003-0003-000000000004', 'b0b0b0b0-0003-0003-0003-000000000003', 'a0a0a0a0-0003-0003-0003-000000000003',  -180.00, 'EUR', 'utilities',      'sepa_dd','IT99B0000099003000000000004', NULL,                                    NOW() - INTERVAL '2 days',  CURRENT_DATE - 2,  true,  'pmi_explicit_stress'),

-- ── pre_churn (Andrea, cust 0005) — crypto + internal_transfer ──
('f0f0f0f0-0005-0005-0005-000000000001', 'b0b0b0b0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005',  -800.00, 'EUR', 'crypto_exchange','wire',   'IT99B0000099005000000000001', NULL,                                    NOW() - INTERVAL '12 days', CURRENT_DATE - 12, false, 'churn_exploration'),
('f0f0f0f0-0005-0005-0005-000000000002', 'b0b0b0b0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005', -2500.00, 'EUR', 'internal_transfer','wire', 'IT99B0000099005000000000099', NULL,                                    NOW() - INTERVAL '6 days',  CURRENT_DATE - 6,  false, 'churn_decision'),
('f0f0f0f0-0005-0005-0005-000000000003', 'b0b0b0b0-0005-0005-0005-000000000005', 'a0a0a0a0-0005-0005-0005-000000000005',   -45.00, 'EUR', 'grocery',        'pos',    NULL,                         'c0c0c0c0-0005-0005-0005-000000000005',   NOW() - INTERVAL '2 days',  CURRENT_DATE - 2,  false, 'churn_emotional_distance'),

-- ── investment (Stefano, cust 0007) — accumulo + interesse app ──
('f0f0f0f0-0007-0007-0007-000000000001', 'b0b0b0b0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', 8500.00, 'EUR', 'salary_income',  'wire',   'IT99B0000099007000000000001', NULL,                                    NOW() - INTERVAL '14 days', CURRENT_DATE - 14, true,  'investment_accumulation'),
('f0f0f0f0-0007-0007-0007-000000000002', 'b0b0b0b0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', 6000.00, 'EUR', 'salary_income',  'wire',   'IT99B0000099007000000000001', NULL,                                    NOW() - INTERVAL '7 days',  CURRENT_DATE - 7,  true,  'investment_accumulation'),
('f0f0f0f0-0007-0007-0007-000000000003', 'b0b0b0b0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007', -1200.00,'EUR', 'investment',     'wire',   'IT99B0000099007000000000002', NULL,                                    NOW() - INTERVAL '3 days',  CURRENT_DATE - 3,  false, 'investment_interest'),
('f0f0f0f0-0007-0007-0007-000000000004', 'b0b0b0b0-0007-0007-0007-000000000007', 'a0a0a0a0-0007-0007-0007-000000000007',  -220.00,'EUR', 'restaurant_cafe','pos',    NULL,                         'c0c0c0c0-0007-0007-0007-000000000007',   NOW() - INTERVAL '1 day',   CURRENT_DATE - 1,  false, 'investment_accumulation'),

-- ── ordinary (Luca, cust 0009) — spese baseline normali ──
('f0f0f0f0-0009-0009-0009-000000000001', 'b0b0b0b0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009',  1800.00, 'EUR', 'salary_income',  'wire',   'IT99B0000099009000000000001', NULL,                                    NOW() - INTERVAL '10 days', CURRENT_DATE - 10, true,  'ordinary_salary_cycle'),
('f0f0f0f0-0009-0009-0009-000000000002', 'b0b0b0b0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009',   -95.00, 'EUR', 'grocery',        'pos',    NULL,                         'c0c0c0c0-0009-0009-0009-000000000009',   NOW() - INTERVAL '5 days',  CURRENT_DATE - 5,  false, 'ordinary_baseline'),
('f0f0f0f0-0009-0009-0009-000000000003', 'b0b0b0b0-0009-0009-0009-000000000009', 'a0a0a0a0-0009-0009-0009-000000000009',   -65.00, 'EUR', 'utilities',      'sepa_dd',NULL,                         NULL,                                    NOW() - INTERVAL '2 days',  CURRENT_DATE - 2,  true,  'ordinary_baseline');


-- ============================================================
-- APP_EVENTS — 2-3 per cliente, coerenti con il pattern
-- ============================================================

INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES

-- real_estate: simulazioni mutuo ripetute (segnale chiave)
('a1a1a1a1-0001-0001-0001-000000000001', 'a0a0a0a0-0001-0001-0001-000000000001', 'screen_view', 'mutui/simulazione', gen_random_uuid(), 245, NOW() - INTERVAL '8 days',  'ios',     true,  'commercial',  5, false),
('a1a1a1a1-0001-0001-0001-000000000002', 'a0a0a0a0-0001-0001-0001-000000000001', 'screen_view', 'mutui/simulazione', gen_random_uuid(), 312, NOW() - INTERVAL '5 days',  'ios',     true,  'commercial',  6, true),
('a1a1a1a1-0001-0001-0001-000000000003', 'a0a0a0a0-0001-0001-0001-000000000001', 'screen_view', 'mutui/offerte',     gen_random_uuid(), 198, NOW() - INTERVAL '2 days',  'ios',     false, 'commercial',  4, true),

-- pmi_deterioration: sessions normali
('a1a1a1a1-0003-0003-0003-000000000001', 'a0a0a0a0-0003-0003-0003-000000000003', 'screen_view', 'home',              gen_random_uuid(), 88,  NOW() - INTERVAL '7 days',  'android', false, 'essential',   2, false),
('a1a1a1a1-0003-0003-0003-000000000002', 'a0a0a0a0-0003-0003-0003-000000000003', 'screen_view', 'movimenti',         gen_random_uuid(), 95,  NOW() - INTERVAL '3 days',  'android', false, 'essential',   3, true),

-- pre_churn: solo essential, sessioni brevi, push ignorate
('a1a1a1a1-0005-0005-0005-000000000001', 'a0a0a0a0-0005-0005-0005-000000000005', 'screen_view', 'saldo',             gen_random_uuid(), 42,  NOW() - INTERVAL '10 days', 'ios',     false, 'essential',   1, false),
('a1a1a1a1-0005-0005-0005-000000000002', 'a0a0a0a0-0005-0005-0005-000000000005', 'screen_view', 'home',              gen_random_uuid(), 38,  NOW() - INTERVAL '4 days',  'ios',     false, 'essential',   1, false),
('a1a1a1a1-0005-0005-0005-000000000003', 'a0a0a0a0-0005-0005-0005-000000000005', 'screen_view', 'movimenti',         gen_random_uuid(), 35,  NOW() - INTERVAL '1 day',   'ios',     false, 'essential',   2, false),

-- investment: simulazioni investimento ripetute
('a1a1a1a1-0007-0007-0007-000000000001', 'a0a0a0a0-0007-0007-0007-000000000007', 'screen_view', 'investimenti/fondi',gen_random_uuid(), 320, NOW() - INTERVAL '6 days',  'ios',     true,  'commercial',  7, false),
('a1a1a1a1-0007-0007-0007-000000000002', 'a0a0a0a0-0007-0007-0007-000000000007', 'screen_view', 'investimenti/fondi',gen_random_uuid(), 285, NOW() - INTERVAL '3 days',  'ios',     true,  'commercial',  6, true),
('a1a1a1a1-0007-0007-0007-000000000003', 'a0a0a0a0-0007-0007-0007-000000000007', 'screen_view', 'investimenti/simulatore', gen_random_uuid(), 410, NOW() - INTERVAL '1 day', 'ios', true,  'exploratory', 5, true),

-- ordinary: navigazione normale
('a1a1a1a1-0009-0009-0009-000000000001', 'a0a0a0a0-0009-0009-0009-000000000009', 'screen_view', 'home',              gen_random_uuid(), 125, NOW() - INTERVAL '5 days',  'ios',     true,  'essential',   4, false),
('a1a1a1a1-0009-0009-0009-000000000002', 'a0a0a0a0-0009-0009-0009-000000000009', 'screen_view', 'movimenti',         gen_random_uuid(), 142, NOW() - INTERVAL '2 days',  'ios',     false, 'essential',   3, true);


-- ============================================================
-- MARKET_DATA — 4 metriche, valori realistici 2025
-- ============================================================

INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value, recorded_at, source)
VALUES
    ('b1b1b1b1-0001-0001-0001-000000000001', 'ecb_rate',  'ecb_rate',        3.65000, 3.90000, NOW() - INTERVAL '1 day',  'synthetic'),
    ('b1b1b1b1-0002-0002-0002-000000000002', 'index',     'btp_bund_spread', 1.42000, 1.38000, NOW() - INTERVAL '1 day',  'synthetic'),
    ('b1b1b1b1-0003-0003-0003-000000000003', 'irs_curve', 'irs_10y',         3.12000, 3.18000, NOW() - INTERVAL '1 day',  'synthetic'),
    ('b1b1b1b1-0004-0004-0004-000000000004', 'inflation', 'inflation_rate',  2.10000, 2.30000, NOW() - INTERVAL '1 day',  'synthetic');


-- ============================================================
-- VERIFICA
-- ============================================================
SELECT 'customers'    AS tabella, COUNT(*) AS righe FROM smash_own.customers
UNION ALL
SELECT 'accounts',    COUNT(*) FROM smash_own.accounts
UNION ALL
SELECT 'cards',       COUNT(*) FROM smash_own.cards
UNION ALL
SELECT 'loans',       COUNT(*) FROM smash_own.loans
UNION ALL
SELECT 'crm_profiles',COUNT(*) FROM smash_own.crm_profiles
UNION ALL
SELECT 'transactions',COUNT(*) FROM smash_own.transactions
UNION ALL
SELECT 'app_events',  COUNT(*) FROM smash_own.app_events
UNION ALL
SELECT 'market_data', COUNT(*) FROM smash_own.market_data
ORDER BY tabella;