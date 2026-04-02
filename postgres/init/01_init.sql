-- ============================================================
-- 01_init.sql
-- Eseguito automaticamente da PostgreSQL al primo avvio
-- nel database 'smash' (definito da POSTGRES_DB nel compose)
-- NON serve \connect — PostgreSQL esegue già in smash
-- ============================================================

-- ------------------------------------------------------------
-- 1. Utenti
-- ------------------------------------------------------------

CREATE USER smash_own WITH
    LOGIN
    PASSWORD 'smash_own' REPLICATION;

CREATE USER smash_app WITH
    LOGIN
    PASSWORD 'smash_app';

CREATE USER smash_repl WITH
    LOGIN
    PASSWORD 'smash_repl' REPLICATION;
-- ------------------------------------------------------------
-- 2. Owner del database
-- PostgreSQL ha creato 'smash' owned da postgres (superuser)
-- lo trasferiamo a smash_own
-- ------------------------------------------------------------

ALTER DATABASE smash OWNER TO smash_own;

-- ------------------------------------------------------------
-- 3. Schema
-- ------------------------------------------------------------

CREATE SCHEMA smash_own
    AUTHORIZATION smash_own;

ALTER DATABASE smash SET search_path TO smash_own, public;

-- ------------------------------------------------------------
-- 4. Permessi sullo schema
-- ------------------------------------------------------------

GRANT ALL PRIVILEGES ON SCHEMA smash_own TO smash_own;
GRANT USAGE ON SCHEMA smash_own TO smash_app;
GRANT USAGE ON SCHEMA smash_own TO smash_repl;

-- ------------------------------------------------------------
-- 5. Default privileges
-- Ogni tabella creata da smash_own → smash_app ottiene DML
-- Ogni tabella creata da smash_own → smash_repl ottiene SELECT
-- Senza questo ogni nuova tabella richiederebbe un grant manuale
-- ------------------------------------------------------------

ALTER DEFAULT PRIVILEGES
    FOR USER postgres
    IN SCHEMA smash_own
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO smash_app;

ALTER DEFAULT PRIVILEGES
    FOR USER postgres
    IN SCHEMA smash_own
    GRANT USAGE, SELECT ON SEQUENCES TO smash_app;

ALTER DEFAULT PRIVILEGES
    FOR USER postgres
    IN SCHEMA smash_own
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO smash_repl;
