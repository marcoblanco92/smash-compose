SET search_path TO smash_own;

-- 1. Pulizia
DELETE FROM transactions WHERE customer_id IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'b22ebc99-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'c33ebc99-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid);
DELETE FROM accounts     WHERE customer_id IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'b22ebc99-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'c33ebc99-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid);
DELETE FROM customers    WHERE customer_id IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'b22ebc99-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'c33ebc99-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid);

-- 2. Inserimento Clienti
INSERT INTO customers (customer_id, first_name, last_name, birth_date, birth_place, tax_code, segment, pattern_type, onboarding_date, is_active)
VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'Mario', 'Rossi', '1985-05-20', 'Roma', 'RSSMRA85E20H501Z', 'retail', 'ordinary', CURRENT_DATE - INTERVAL '2 years', true),
    ('b22ebc99-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'Luigi', 'Verdi', '1990-07-15', 'Milano', 'VRDLGU90L15F205A', 'retail', 'ordinary', CURRENT_DATE - INTERVAL '2 years', true),
    ('c33ebc99-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid, 'Anna', 'Bianchi', '1982-03-10', 'Napoli', 'BNCNNA82C50F839U', 'retail', 'ordinary', CURRENT_DATE - INTERVAL '2 years', true);

-- 3. Inserimento Conti (UUID validi senza prefissi custom)
INSERT INTO accounts (account_id, customer_id, account_type, iban, current_balance, opened_date, status)
VALUES
    ('11111111-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'checking', 'IT99L01234567890123456789', 5000.00, CURRENT_DATE - INTERVAL '2 years', 'active'),
    ('22222222-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'b22ebc99-9c0b-4ef8-bb6d-6bb9bd380b22'::uuid, 'checking', 'IT88M01234567890123456788', 7500.00, CURRENT_DATE - INTERVAL '2 years', 'active'),
    ('33333333-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid, 'c33ebc99-9c0b-4ef8-bb6d-6bb9bd380c33'::uuid, 'checking', 'IT77N01234567890123456787', 3200.00, CURRENT_DATE - INTERVAL '2 years', 'active');

-- 4. Inserimento Transazioni Massive
INSERT INTO transactions (
    transaction_id, account_id, customer_id, amount, currency,
    merchant_category, channel, counterpart, transaction_date, value_date, description
)
SELECT
    gen_random_uuid(),
    acc.account_id,
    c.customer_id,
    (random() * 200 + 10)::numeric(15,2) * (CASE WHEN random() > 0.1 THEN -1 ELSE 2.5 END),
    'EUR',
    (ARRAY['food', 'tech', 'leisure', 'utilities', 'travel', 'health'])[floor(random() * 6 + 1)],
    (ARRAY['pos', 'online', 'atm', 'wire', 'instant'])[floor(random() * 5 + 1)]::transaction_channel,
    'TOKEN-' || floor(random() * 500),
    (NOW() - (day_val || ' days')::interval) - (random() * interval '23 hours'),
    (NOW() - (day_val || ' days')::interval)::date,
    'Tx day ' || day_val
FROM
    customers c
        JOIN
    accounts acc ON c.customer_id = acc.customer_id
        CROSS JOIN
    generate_series(0, 360) AS day_val
        CROSS JOIN
    generate_series(1, 5) AS txn_num
WHERE
    random() < 0.7;