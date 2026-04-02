-- Usiamo un UUID fisso per rintracciarlo facilmente: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
SET search_path TO smash_own;

-- 1. Pulizia atomica (rispetta l'ordine delle FK)
DELETE FROM transactions WHERE customer_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
DELETE FROM accounts     WHERE customer_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
DELETE FROM customers    WHERE customer_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;

-- 2. Inserimento Cliente
INSERT INTO customers (
    customer_id, first_name, last_name, birth_date, birth_place,
    tax_code, segment, pattern_type, onboarding_date, is_active
) VALUES (
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid,
             'Mario', 'Rossi', '1985-05-20', 'Roma',
             'RSSMRA85E20H501Z', 'retail', 'ordinary', CURRENT_DATE - INTERVAL '2 years', true
         );

-- 3. Inserimento Conto
INSERT INTO accounts (
    account_id, customer_id, account_type, iban, current_balance, opened_date, status
) VALUES (
             'b11ebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::uuid,
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid,
             'checking', 'IT99L01234567890123456789', 5000.00, CURRENT_DATE - INTERVAL '2 years', 'active'
         );

-- 4. Inserimento 180 Transazioni (una ogni 2 giorni per coprire w365)
INSERT INTO transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, transaction_date, value_date, description
)
SELECT
    gen_random_uuid(),
    'b11ebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::uuid,
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid,
    (random() * 350 + 5)::numeric(15,2) * (CASE WHEN random() > 0.15 THEN -1 ELSE 3.5 END),
    'EUR',
    (ARRAY['food', 'tech', 'leisure', 'utilities', 'travel', 'health'])[floor(random() * 6 + 1)],
    (ARRAY['pos', 'online', 'atm', 'wire', 'instant'])[floor(random() * 5 + 1)]::transaction_channel,
    'TOKEN-' || floor(random() * 100),
    NOW() - (val || ' days')::interval,
    (NOW() - (val || ' days')::interval)::date,
    'Test transaction day ' || val
FROM generate_series(0, 360, 2) AS val;