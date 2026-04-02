-- ============================================================
-- 02_insert_second_transaction.sql
-- Aggiunge una seconda transazione per il cliente di test.
-- Da eseguire DOPO 01_insert_test_client.sql.
--
-- Scopo: verificare che Flink processi nuovi eventi CDC live
--        per un cliente già presente in RocksDB.
--
-- Uso:
--   psql -h localhost -U smash_own -d smash -f 02_insert_second_transaction.sql
-- ============================================================

SET search_path TO smash_own;

-- ── SECONDA TRANSAZIONE ───────────────────────────────────────
-- Categoria investment → deve triggerare investmentViews nel profilo
INSERT INTO smash_own.transactions
(transaction_id, account_id, customer_id, amount, currency,
 merchant_category, channel, counterpart, card_id,
 transaction_date, value_date, is_recurring, pattern_phase)
VALUES (
           'dc0001dd-0000-0000-0000-000000000002',
           'dc0001bb-0000-0000-0000-000000000001',
           'dc0001aa-0000-0000-0000-000000000001',
           1200.00, 'EUR',
           'investment',
           'wire',
           'IT99Z0000099000DC0001EXT002',
           NULL,
           NOW(),
           CURRENT_DATE,
           false,
           'investment_interest'
       );

-- ── VERIFICA ──────────────────────────────────────────────────
SELECT
    transaction_id,
    amount,
    merchant_category,
    channel,
    transaction_date,
    pattern_phase
FROM smash_own.transactions
WHERE customer_id = 'dc0001aa-0000-0000-0000-000000000001'
ORDER BY transaction_date;