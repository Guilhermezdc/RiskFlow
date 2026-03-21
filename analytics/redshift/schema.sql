-- ============================================================
-- analytics/redshift/schema.sql
-- DDLs completos para as 3 camadas do Data Warehouse
-- Executar em ordem: raw → staging → mart
-- ============================================================

-- ──────────────────────────────────────────────
-- SCHEMAS
-- ──────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;


-- ============================================================
-- CAMADA RAW — dados exatamente como chegam do S3
-- Sem transformações. Apenas tipagem básica.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id      VARCHAR(36)     NOT NULL,
    customer_id         VARCHAR(50),
    account_id          VARCHAR(50),
    amount              DECIMAL(15,2),
    currency            VARCHAR(3),
    transaction_type    VARCHAR(20),
    merchant_id         VARCHAR(50),
    merchant_category   VARCHAR(10),
    merchant_country    VARCHAR(2),
    device_id           VARCHAR(100),
    device_type         VARCHAR(20),
    ip_address          VARCHAR(45),
    country_code        VARCHAR(2),
    is_international    BOOLEAN,
    is_new_device       BOOLEAN,
    channel             VARCHAR(20),
    timestamp           VARCHAR(30),
    event_time_ms       BIGINT,
    -- Partição
    year                SMALLINT,
    month               SMALLINT,
    day                 SMALLINT,
    hour                SMALLINT
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (event_time_ms);


CREATE TABLE IF NOT EXISTS raw.decisions (
    transaction_id          VARCHAR(36)     NOT NULL,
    customer_id             VARCHAR(50),
    amount                  DECIMAL(15,2),
    currency                VARCHAR(3),
    status                  VARCHAR(20),
    risk_score              DECIMAL(5,4),
    triggered_rules         VARCHAR(500),   -- JSON array serializado
    transaction_timestamp   VARCHAR(30),
    decision_timestamp      VARCHAR(30),
    processing_latency_ms   INTEGER,
    -- Partição
    year                    SMALLINT,
    month                   SMALLINT,
    day                     SMALLINT,
    hour                    SMALLINT
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (decision_timestamp);


-- ============================================================
-- CAMADA STAGING — dados limpos e joinados
-- Fact table central do DW.
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.fact_transactions (
    transaction_id          VARCHAR(36)     NOT NULL,
    customer_id             VARCHAR(50)     NOT NULL,
    account_id              VARCHAR(50),
    amount                  DECIMAL(15,2)   NOT NULL,
    currency                VARCHAR(3),
    transaction_type        VARCHAR(20),
    merchant_id             VARCHAR(50),
    merchant_category       VARCHAR(10),
    merchant_country        VARCHAR(2),
    device_id               VARCHAR(100),
    device_type             VARCHAR(20),
    country_code            VARCHAR(2),
    is_international        BOOLEAN,
    is_new_device           BOOLEAN,
    channel                 VARCHAR(20),
    -- Decision (join)
    decision_status         VARCHAR(20),
    risk_score              DECIMAL(5,4),
    triggered_rules         VARCHAR(500),
    processing_latency_ms   INTEGER,
    decision_timestamp      VARCHAR(30),
    -- Temporal
    event_date              DATE,
    event_hour              SMALLINT,
    event_time_ms           BIGINT,
    -- Metadados ETL
    loaded_at               TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (event_date, customer_id);

-- Constraint de unicidade (Redshift não enforça, mas documenta a intenção)
-- ALTER TABLE staging.fact_transactions ADD CONSTRAINT pk_fact_tx PRIMARY KEY (transaction_id);


-- ============================================================
-- CAMADA MART — tabelas analíticas para BI / dashboards
-- ============================================================

-- ── Resumo diário de risco ──
CREATE TABLE IF NOT EXISTS mart.daily_risk_summary (
    event_date              DATE            NOT NULL,
    decision_status         VARCHAR(20)     NOT NULL,
    transaction_count       INTEGER,
    total_amount            DECIMAL(18,2),
    avg_amount              DECIMAL(12,2),
    avg_risk_score          DECIMAL(5,4),
    avg_latency_ms          DECIMAL(10,2),
    p95_latency_ms          INTEGER,
    loaded_at               TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE ALL
SORTKEY (event_date, decision_status);


-- ── Perfil de risco por cliente ──
CREATE TABLE IF NOT EXISTS mart.customer_risk_profile (
    customer_id             VARCHAR(50)     NOT NULL,
    total_transactions      INTEGER,
    total_amount            DECIMAL(18,2),
    avg_amount              DECIMAL(12,2),
    max_amount              DECIMAL(15,2),
    avg_risk_score          DECIMAL(5,4),
    max_risk_score          DECIMAL(5,4),
    total_rejections        INTEGER,
    total_reviews           INTEGER,
    rejection_rate          DECIMAL(5,4),
    last_transaction_date   DATE,
    loaded_at               TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id);


-- ── Efetividade das regras ──
CREATE TABLE IF NOT EXISTS mart.rule_effectiveness (
    rule                            VARCHAR(100)    NOT NULL,
    event_date                      DATE            NOT NULL,
    trigger_count                   INTEGER,
    avg_risk_score_when_triggered   DECIMAL(5,4),
    led_to_reject                   INTEGER,
    led_to_review                   INTEGER,
    loaded_at                       TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE ALL
SORTKEY (event_date, rule);


-- ── SLA / Latência de processamento ──
CREATE TABLE IF NOT EXISTS mart.sla_monitoring (
    event_date              DATE            NOT NULL,
    event_hour              SMALLINT        NOT NULL,
    total_decisions         INTEGER,
    avg_latency_ms          DECIMAL(10,2),
    p50_latency_ms          INTEGER,
    p95_latency_ms          INTEGER,
    p99_latency_ms          INTEGER,
    sla_breach_count        INTEGER,        -- transações > 2000ms
    sla_breach_rate         DECIMAL(5,4),
    loaded_at               TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE ALL
SORTKEY (event_date, event_hour);


-- ============================================================
-- VIEWS analíticas úteis
-- ============================================================

-- Taxa de fraude nas últimas 24h (para dashboard operacional)
CREATE OR REPLACE VIEW mart.v_fraud_rate_24h AS
SELECT
    event_date,
    SUM(transaction_count) AS total_transactions,
    SUM(CASE WHEN decision_status = 'REJECT' THEN transaction_count ELSE 0 END) AS rejected,
    SUM(CASE WHEN decision_status = 'MANUAL_REVIEW' THEN transaction_count ELSE 0 END) AS manual_review,
    SUM(CASE WHEN decision_status = 'APPROVE' THEN transaction_count ELSE 0 END) AS approved,
    ROUND(
        SUM(CASE WHEN decision_status = 'REJECT' THEN transaction_count ELSE 0 END)::FLOAT
        / NULLIF(SUM(transaction_count), 0), 4
    ) AS reject_rate
FROM mart.daily_risk_summary
WHERE event_date >= CURRENT_DATE - 1
GROUP BY event_date;


-- Top 10 regras que mais disparam
CREATE OR REPLACE VIEW mart.v_top_rules AS
SELECT
    rule,
    SUM(trigger_count) AS total_triggers,
    AVG(avg_risk_score_when_triggered) AS avg_risk_score,
    SUM(led_to_reject) AS total_rejects,
    SUM(led_to_review) AS total_reviews
FROM mart.rule_effectiveness
WHERE event_date >= CURRENT_DATE - 30
GROUP BY rule
ORDER BY total_triggers DESC
LIMIT 10;
