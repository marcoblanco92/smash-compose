-- ============================================================
-- 03_publication.sql
-- Crea la publication per Debezium CDC
-- Gira nel database 'smash' automaticamente
-- NON serve \connect
-- Eseguito dopo 02_create_tables.sql — le tabelle esistono già
-- ============================================================

SET search_path TO smash_own;

CREATE PUBLICATION smash_debezium
    FOR ALL TABLES
    WITH (publish = 'insert, update, delete');

-- Verifica
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
FROM pg_publication
WHERE pubname = 'smash_debezium';