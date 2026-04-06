-- ============================================================
-- 00_cleanup.sql
-- Azzera tutte le 8 tabelle in ordine inverso rispetto alle FK.
-- ⚠️  SOLO ambiente di sviluppo/test.
-- ============================================================

SET search_path TO smash_own;

TRUNCATE TABLE
    smash_own.app_events,
    smash_own.market_data,
    smash_own.crm_profiles,
    smash_own.loans,
    smash_own.transactions,
    smash_own.cards,
    smash_own.accounts,
    smash_own.customers
CASCADE;

SELECT relname AS tabella, n_live_tup AS righe_stimate
FROM pg_stat_user_tables
WHERE schemaname = 'smash_own'
ORDER BY relname;
