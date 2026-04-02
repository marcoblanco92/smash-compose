-- ============================================================
-- 01_insert_test_client.sql
-- Inserisce 1 cliente completo con 1 elemento per tabella.
-- UUID fisso per poter referenziare nel secondo script.
--
-- Uso:
--   psql -h localhost -U smash_own -d smash -f 01_insert_test_client.sql
-- ============================================================

SET search_path TO smash_own;

TRUNCATE smash_own.app_events      CASCADE;
TRUNCATE smash_own.transactions    CASCADE;
TRUNCATE smash_own.cards           CASCADE;
TRUNCATE smash_own.loans           CASCADE;
TRUNCATE smash_own.crm_profiles    CASCADE;
TRUNCATE smash_own.accounts        CASCADE;
TRUNCATE smash_own.market_data     CASCADE;
TRUNCATE smash_own.customers       CASCADE;

-- UUID fissi — referenziati anche in 02_insert_second_transaction.sql
-- customer_id : dc0001aa-0000-0000-0000-000000000001
-- account_id  : dc0001bb-0000-0000-0000-000000000001
-- card_id     : dc0001cc-0000-0000-0000-000000000001

-- ── 1. CUSTOMER ───────────────────────────────────────────────
INSERT INTO smash_own.customers
(customer_id, first_name, last_name, birth_date, birth_place, tax_code,
 segment, pattern_type, pattern_trigger_date, active_pattern,
 risk_class, clv_score, onboarding_date, is_active)
VALUES (
           'dc0001aa-0000-0000-0000-000000000001',
           'Test', 'Debug',
           '1985-06-15', 'Milano', 'DBGTST85H15F205X',
           'retail',
           'investment_opportunity',
           CURRENT_DATE - 14,
           'investment_opportunity',
           'low', 75.00,
           CURRENT_DATE - 365,
           true
       );

-- ── 2. ACCOUNT ────────────────────────────────────────────────
INSERT INTO smash_own.accounts
(account_id, customer_id, account_type, iban, currency,
 current_balance, opened_date, status, overdraft_limit)
VALUES (
           'dc0001bb-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           'checking',
           'IT00X0000000000DC0001BB0001',
           'EUR',
           45000.00,
           CURRENT_DATE - 365,
           'active',
           500.00
       );

-- ── 3. CARD ───────────────────────────────────────────────────
INSERT INTO smash_own.cards
(card_id, customer_id, account_id, card_type, card_number,
 plafond_limit, plafond_used, billing_cycle_day, status, issued_date, expiry_date)
VALUES (
           'dc0001cc-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           'dc0001bb-0000-0000-0000-000000000001',
           'debit',
           '4111000000000001',
           NULL, 0.00, NULL,
           'active',
           CURRENT_DATE - 365,
           CURRENT_DATE + 1095
       );

-- ── 4. TRANSACTION (prima) ────────────────────────────────────
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, card_id,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES (
           'dc0001dd-0000-0000-0000-000000000001',
           'dc0001bb-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           3500.00, 'EUR',
           'salary_income',
           'wire',
           'IT99Z0000099000DC0001EXT001',
           NULL,
           NOW() - INTERVAL '5 days',
           CURRENT_DATE - 5,
           true,
           'investment_accumulation'
       );

-- ── 5. LOAN ───────────────────────────────────────────────────
INSERT INTO smash_own.loans
(loan_id, customer_id, loan_type, principal_amount, outstanding_balance,
 interest_rate, start_date, maturity_date, next_due_date,
 days_past_due, credit_line_usage_pct, avg_payment_delay_days,
 status, collateral_type)
VALUES (
           'dc0001ee-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           'personal',
           10000.00, 6500.00,
           5.500,
           CURRENT_DATE - 365,
           CURRENT_DATE + 730,
           CURRENT_DATE + 30,
           0, NULL, 0,
           'active', 'none'
       );

-- ── 6. CRM PROFILE ────────────────────────────────────────────
INSERT INTO smash_own.crm_profiles
(profile_id, customer_id, segment, products_held, has_mortgage,
 has_investments, clv_score, churn_risk_score, preferred_channel,
 push_opt_in, avg_session_duration_30d, push_ignore_streak,
 days_since_last_contact, product_usage_score)
VALUES (
           'dc0001ff-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           'retail',
           '["checking","personal_loan"]',
           false, false,
           75.00, 0.080,
           'app', true,
           210, 0, 10, 0.720
       );

-- ── 7. APP EVENT ──────────────────────────────────────────────
INSERT INTO smash_own.app_events
(event_id, customer_id, event_type, screen_name, session_id,
 session_duration_s, event_timestamp, device_type, is_push_opened,
 feature_category, screens_visited_n, is_return_visit)
VALUES (
           'dc000177-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           'screen_view',
           'investimenti/fondi',
           gen_random_uuid(),
           320,
           NOW() - INTERVAL '3 days',
           'ios', true,
           'commercial',
           6, false
       );

-- ── 8. MARKET DATA ────────────────────────────────────────────
INSERT INTO smash_own.market_data
(record_id, data_type, metric_name, value, previous_value, recorded_at, source)
VALUES (
           'dc000188-0000-0000-0000-000000000001',
           'ecb_rate',
           'ecb_rate',
           3.65000, 3.90000,
           NOW() - INTERVAL '1 day',
           'synthetic'
       );

-- ── VERIFICA ──────────────────────────────────────────────────
SELECT 'customers'    AS tabella, COUNT(*) AS righe
FROM smash_own.customers    WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'accounts',   COUNT(*) FROM smash_own.accounts     WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'cards',      COUNT(*) FROM smash_own.cards        WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'transactions',COUNT(*) FROM smash_own.transactions WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'loans',      COUNT(*) FROM smash_own.loans        WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'crm_profiles',COUNT(*) FROM smash_own.crm_profiles WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
UNION ALL
SELECT 'app_events', COUNT(*) FROM smash_own.app_events   WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001';