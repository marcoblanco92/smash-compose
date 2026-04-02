-- ============================================================
-- 02_create_tables.sql
-- DDL tabelle — gira nel database 'smash' automaticamente
-- NON serve \connect — POSTGRES_DB=smash nel compose lo garantisce
-- Lo script gira come utente postgres (superuser)
-- Le tabelle vengono create nello schema smash_own
-- e poi trasferite a smash_own tramite ALTER TABLE ... OWNER
--
-- Ordine creazione (rispetta FK):
--   1. customers
--   2. accounts       (FK → customers)
--   3. cards          (FK → customers, accounts)
--   4. transactions   (FK → accounts, customers, cards)
--   5. loans          (FK → customers)
--   6. crm_profiles   (FK → customers)
--   7. app_events     (FK → customers)
--   8. market_data    (nessuna FK)
-- ============================================================

SET search_path TO smash_own;

-- ============================================================
-- ENUM TYPES
-- ============================================================

CREATE TYPE segment_type AS ENUM ('retail', 'affluent', 'pmi');

CREATE TYPE pattern_type AS ENUM (
    'real_estate',
    'pmi_deterioration',
    'investment_opportunity',
    'pre_churn',
    'ordinary'
);

CREATE TYPE active_pattern_type AS ENUM (
    'real_estate',
    'pmi_deterioration',
    'pre_churn',
    'investment_opportunity',
    'ordinary'
);

CREATE TYPE risk_class_type AS ENUM ('low', 'medium', 'high');

CREATE TYPE account_type AS ENUM ('checking', 'savings', 'business');

CREATE TYPE account_status AS ENUM ('active', 'dormant', 'closed');

CREATE TYPE transaction_channel AS ENUM (
    'wire', 'pos', 'atm', 'online', 'sepa_dd', 'instant'
);

CREATE TYPE loan_type AS ENUM (
    'mortgage', 'personal', 'business', 'credit_line'
);

CREATE TYPE loan_status AS ENUM (
    'active', 'closed', 'defaulted', 'restructured'
);

CREATE TYPE collateral_type AS ENUM (
    'property', 'vehicle', 'securities', 'none'
);

CREATE TYPE preferred_channel_type AS ENUM (
    'app', 'branch', 'phone', 'email'
);

CREATE TYPE device_type AS ENUM ('ios', 'android', 'web');

CREATE TYPE market_data_type AS ENUM (
    'ecb_rate', 'irs_curve', 'index', 'inflation'
);

CREATE TYPE feature_category_type AS ENUM (
    'essential', 'exploratory', 'commercial'
);

CREATE TYPE card_type AS ENUM ('credit', 'debit', 'prepaid');

CREATE TYPE card_status AS ENUM ('active', 'blocked', 'expired', 'cancelled');


-- ============================================================
-- TABELLA 1: customers
-- PII: first_name, last_name, birth_date, birth_place, tax_code
-- Droppati dalla SMT Debezium prima di Kafka
-- ============================================================

CREATE TABLE smash_own.customers (
                                     customer_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- ── PII — droppati dalla SMT Debezium ──────────────────
                                     first_name           VARCHAR(50)         NOT NULL,
                                     last_name            VARCHAR(50)         NOT NULL,
                                     birth_date           DATE                NOT NULL,
                                     birth_place          VARCHAR(100)        NOT NULL,
                                     tax_code             VARCHAR(16)         NOT NULL UNIQUE,
    -- ────────────────────────────────────────────────────────

                                     segment              segment_type        NOT NULL,
                                     pattern_type         pattern_type        NOT NULL,
                                     pattern_trigger_date DATE,
                                     active_pattern       active_pattern_type NOT NULL DEFAULT 'ordinary',
                                     risk_class           risk_class_type     NOT NULL DEFAULT 'low',
                                     clv_score            NUMERIC(5,2)        NOT NULL DEFAULT 0.00,
                                     relationship_mgr     UUID,
                                     onboarding_date      DATE                NOT NULL,
                                     is_active            BOOLEAN             NOT NULL DEFAULT TRUE,
                                     created_at           TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_segment        ON smash_own.customers(segment);
CREATE INDEX idx_customers_pattern_type   ON smash_own.customers(pattern_type);
CREATE INDEX idx_customers_active_pattern ON smash_own.customers(active_pattern);
CREATE INDEX idx_customers_relationship   ON smash_own.customers(relationship_mgr);
CREATE INDEX idx_customers_tax_code       ON smash_own.customers(tax_code);


-- ============================================================
-- TABELLA 2: accounts
-- PII: iban — mascherato da SMT Debezium (HMAC-SHA256)
-- ============================================================

CREATE TABLE smash_own.accounts (
                                    account_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                    customer_id      UUID           NOT NULL REFERENCES smash_own.customers(customer_id),
                                    account_type     account_type   NOT NULL,
                                    iban             VARCHAR(34)    NOT NULL UNIQUE,  -- PII — mascherato da SMT Debezium
                                    currency         CHAR(3)        NOT NULL DEFAULT 'EUR',
                                    current_balance  NUMERIC(15,2)  NOT NULL DEFAULT 0.00,
                                    opened_date      DATE           NOT NULL,
                                    status           account_status NOT NULL DEFAULT 'active',
                                    overdraft_limit  NUMERIC(10,2)  NOT NULL DEFAULT 0.00,
                                    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_accounts_customer_id ON smash_own.accounts(customer_id);
CREATE INDEX idx_accounts_status      ON smash_own.accounts(status);


-- ============================================================
-- TABELLA 3: cards
-- Creata prima di transactions per rispettare la FK card_id.
-- PII: card_number — mascherato da SMT Debezium (HMAC-SHA256)
-- plafond_limit e billing_cycle_day: NULL per debit e prepaid
-- ============================================================

CREATE TABLE smash_own.cards (
                                 card_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                                 customer_id       UUID         NOT NULL REFERENCES smash_own.customers(customer_id),
                                 account_id        UUID         NOT NULL REFERENCES smash_own.accounts(account_id),
                                 card_type         card_type    NOT NULL,
                                 card_number       VARCHAR(19)  NOT NULL UNIQUE,  -- PII — mascherato da SMT Debezium → card_token
                                 plafond_limit     NUMERIC(10,2),                 -- NULL per debit e prepaid
                                 plafond_used      NUMERIC(10,2) NOT NULL DEFAULT 0.00,
                                 billing_cycle_day SMALLINT,                      -- NULL per debit e prepaid
                                 status            card_status  NOT NULL DEFAULT 'active',
                                 issued_date       DATE         NOT NULL,
                                 expiry_date       DATE,
                                 updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cards_customer_id  ON smash_own.cards(customer_id);
CREATE INDEX idx_cards_account_id   ON smash_own.cards(account_id);
CREATE INDEX idx_cards_status       ON smash_own.cards(status);
CREATE INDEX idx_cards_card_type    ON smash_own.cards(card_type);


-- ============================================================
-- TABELLA 4: transactions
-- PII: counterpart — mascherato da SMT Debezium (HMAC-SHA256)
--      description  — droppato dalla SMT Debezium
-- card_id: NULL per wire, sepa_dd, instant
--          popolato per pos, online, atm
-- pattern_phase: GROUND TRUTH INTERNO — mai nel layer AI
-- ============================================================

CREATE TABLE smash_own.transactions (
                                        transaction_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                        account_id        UUID                NOT NULL REFERENCES smash_own.accounts(account_id),
                                        customer_id       UUID                NOT NULL REFERENCES smash_own.customers(customer_id),
                                        amount            NUMERIC(15,2)       NOT NULL,
                                        currency          CHAR(3)             NOT NULL DEFAULT 'EUR',
                                        merchant_category VARCHAR(50)         NOT NULL,
                                        channel           transaction_channel NOT NULL,
                                        counterpart       VARCHAR(64),                   -- PII — mascherato da SMT Debezium → counterpart_token
                                        card_id           UUID                REFERENCES smash_own.cards(card_id),  -- NULL per wire/sepa_dd/instant
                                        transaction_date  TIMESTAMPTZ         NOT NULL,
                                        value_date        DATE                NOT NULL,
                                        description       TEXT,                          -- PII — droppato dalla SMT Debezium
                                        is_recurring      BOOLEAN             NOT NULL DEFAULT FALSE,
                                        pattern_phase     VARCHAR(50)                    -- GROUND TRUTH INTERNO — mai nel layer AI
);

CREATE INDEX idx_transactions_customer_id   ON smash_own.transactions(customer_id);
CREATE INDEX idx_transactions_account_id    ON smash_own.transactions(account_id);
CREATE INDEX idx_transactions_date          ON smash_own.transactions(transaction_date);
CREATE INDEX idx_transactions_merchant_cat  ON smash_own.transactions(merchant_category);
CREATE INDEX idx_transactions_pattern_phase ON smash_own.transactions(pattern_phase);
CREATE INDEX idx_transactions_channel       ON smash_own.transactions(channel);
CREATE INDEX idx_transactions_cust_date     ON smash_own.transactions(customer_id, transaction_date DESC);
CREATE INDEX idx_transactions_card_id       ON smash_own.transactions(card_id)
    WHERE card_id IS NOT NULL;


-- ============================================================
-- TABELLA 5: loans
-- ============================================================

CREATE TABLE smash_own.loans (
                                 loan_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                 customer_id            UUID            NOT NULL REFERENCES smash_own.customers(customer_id),
                                 loan_type              loan_type       NOT NULL,
                                 principal_amount       NUMERIC(15,2)   NOT NULL,
                                 outstanding_balance    NUMERIC(15,2)   NOT NULL,
                                 interest_rate          NUMERIC(5,3)    NOT NULL,
                                 start_date             DATE            NOT NULL,
                                 maturity_date          DATE,
                                 next_due_date          DATE,
                                 days_past_due          INTEGER         NOT NULL DEFAULT 0,
                                 credit_line_usage_pct  NUMERIC(5,2),
                                 avg_payment_delay_days INTEGER         NOT NULL DEFAULT 0,
                                 status                 loan_status     NOT NULL DEFAULT 'active',
                                 collateral_type        collateral_type NOT NULL DEFAULT 'none',
                                 updated_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_loans_customer_id       ON smash_own.loans(customer_id);
CREATE INDEX idx_loans_type              ON smash_own.loans(loan_type);
CREATE INDEX idx_loans_status            ON smash_own.loans(status);
CREATE INDEX idx_loans_days_past_due     ON smash_own.loans(days_past_due);
CREATE INDEX idx_loans_credit_line_usage ON smash_own.loans(credit_line_usage_pct)
    WHERE loan_type = 'credit_line';


-- ============================================================
-- TABELLA 6: crm_profiles
-- ============================================================

CREATE TABLE smash_own.crm_profiles (
                                        profile_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                        customer_id               UUID                   NOT NULL UNIQUE
                                            REFERENCES smash_own.customers(customer_id),
                                        segment                   VARCHAR(20)            NOT NULL,
                                        products_held             JSONB                  NOT NULL DEFAULT '[]',
                                        has_mortgage              BOOLEAN                NOT NULL DEFAULT FALSE,
                                        has_investments           BOOLEAN                NOT NULL DEFAULT FALSE,
                                        clv_score                 NUMERIC(5,2)           NOT NULL DEFAULT 0.00,
                                        churn_risk_score          NUMERIC(4,3)           NOT NULL DEFAULT 0.000,
                                        relationship_mgr          UUID,
                                        last_contact_date         DATE,
                                        preferred_channel         preferred_channel_type NOT NULL DEFAULT 'app',
                                        push_opt_in               BOOLEAN                NOT NULL DEFAULT TRUE,
                                        avg_session_duration_30d  INTEGER                NOT NULL DEFAULT 0,
                                        push_ignore_streak        SMALLINT               NOT NULL DEFAULT 0,
                                        days_since_last_contact   INTEGER                NOT NULL DEFAULT 0,
                                        product_usage_score       NUMERIC(4,3)           NOT NULL DEFAULT 0.000,
                                        updated_at                TIMESTAMPTZ            NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_crm_customer_id        ON smash_own.crm_profiles(customer_id);
CREATE INDEX idx_crm_segment            ON smash_own.crm_profiles(segment);
CREATE INDEX idx_crm_relationship_mgr   ON smash_own.crm_profiles(relationship_mgr);
CREATE INDEX idx_crm_churn_risk         ON smash_own.crm_profiles(churn_risk_score DESC);
CREATE INDEX idx_crm_push_ignore_streak ON smash_own.crm_profiles(push_ignore_streak DESC);
CREATE INDEX idx_crm_product_usage      ON smash_own.crm_profiles(product_usage_score);


-- ============================================================
-- TABELLA 7: app_events
-- ============================================================

CREATE TABLE smash_own.app_events (
                                      event_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                      customer_id         UUID                   NOT NULL REFERENCES smash_own.customers(customer_id),
                                      event_type          VARCHAR(50)            NOT NULL,
                                      screen_name         VARCHAR(100),
                                      session_id          UUID                   NOT NULL,
                                      session_duration_s  INTEGER                NOT NULL DEFAULT 0,
                                      event_timestamp     TIMESTAMPTZ            NOT NULL,
                                      device_type         device_type            NOT NULL DEFAULT 'ios',
                                      is_push_opened      BOOLEAN,
                                      feature_category    feature_category_type,
                                      screens_visited_n   SMALLINT               NOT NULL DEFAULT 1,
                                      is_return_visit     BOOLEAN                NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_app_events_customer_id      ON smash_own.app_events(customer_id);
CREATE INDEX idx_app_events_timestamp        ON smash_own.app_events(event_timestamp);
CREATE INDEX idx_app_events_session_id       ON smash_own.app_events(session_id);
CREATE INDEX idx_app_events_screen_name      ON smash_own.app_events(screen_name);
CREATE INDEX idx_app_events_feature_category ON smash_own.app_events(feature_category);
CREATE INDEX idx_app_events_cust_ts          ON smash_own.app_events(customer_id, event_timestamp DESC);
CREATE INDEX idx_app_events_return_visit     ON smash_own.app_events(customer_id, is_return_visit)
    WHERE is_return_visit = TRUE;


-- ============================================================
-- TABELLA 8: market_data
-- ============================================================

CREATE TABLE smash_own.market_data (
                                       record_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                       data_type      market_data_type NOT NULL,
                                       metric_name    VARCHAR(50)      NOT NULL,
                                       value          NUMERIC(10,5)    NOT NULL,
                                       previous_value NUMERIC(10,5),
                                       recorded_at    TIMESTAMPTZ      NOT NULL,
                                       source         VARCHAR(20)      NOT NULL DEFAULT 'synthetic'
);

CREATE INDEX idx_market_data_type        ON smash_own.market_data(data_type);
CREATE INDEX idx_market_data_metric      ON smash_own.market_data(metric_name);
CREATE INDEX idx_market_data_recorded_at ON smash_own.market_data(recorded_at DESC);
CREATE INDEX idx_market_data_metric_ts   ON smash_own.market_data(metric_name, recorded_at DESC);


-- ============================================================
-- OWNERSHIP
-- ============================================================

ALTER TABLE smash_own.customers     OWNER TO smash_own;
ALTER TABLE smash_own.accounts      OWNER TO smash_own;
ALTER TABLE smash_own.cards         OWNER TO smash_own;
ALTER TABLE smash_own.transactions  OWNER TO smash_own;
ALTER TABLE smash_own.loans         OWNER TO smash_own;
ALTER TABLE smash_own.crm_profiles  OWNER TO smash_own;
ALTER TABLE smash_own.app_events    OWNER TO smash_own;
ALTER TABLE smash_own.market_data   OWNER TO smash_own;

-- Verifica finale — deve mostrare 8 tabelle tutte owned da smash_own
SELECT tablename, tableowner
FROM pg_tables
WHERE schemaname = 'smash_own'
ORDER BY tablename;