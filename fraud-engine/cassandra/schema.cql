-- ============================================================
-- fraud-engine/cassandra/schema.cql
-- Schema Cassandra para warm memory (histórico 90 dias)
-- ============================================================
-- Executar: cqlsh -f schema.cql

-- Keyspace com replicação simples para dev / ajustar para prod
CREATE KEYSPACE IF NOT EXISTS risk_flow
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1   -- produção: NetworkTopologyStrategy com fator 3
}
AND durable_writes = true;

USE risk_flow;

-- ──────────────────────────────────────────────
-- Transações por cliente (90 dias)
-- Partition key: customer_id + bucket (por mês para evitar partições gigantes)
-- Clustering: timestamp DESC para queries de "últimas N transações"
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_transactions (
    customer_id     TEXT,
    bucket          TEXT,           -- 'YYYY-MM' para distribuir partições por mês
    transaction_id  UUID,
    amount          DECIMAL,
    currency        TEXT,
    transaction_type TEXT,
    merchant_id     TEXT,
    merchant_category TEXT,
    merchant_country TEXT,
    device_id       TEXT,
    country_code    TEXT,
    status          TEXT,           -- APPROVE | REJECT | MANUAL_REVIEW
    risk_score      DECIMAL,
    is_international BOOLEAN,
    event_time      TIMESTAMP,
    PRIMARY KEY ((customer_id, bucket), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND default_time_to_live = 7776000  -- 90 dias em segundos
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 7
  }
  AND compression = {
    'sstable_compression': 'LZ4Compressor'
  };

-- ──────────────────────────────────────────────
-- Perfil agregado do cliente (atualizado a cada transação)
-- Usado para score histórico, médias, padrões
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_profile (
    customer_id             TEXT PRIMARY KEY,
    avg_amount              DECIMAL,
    max_amount              DECIMAL,
    total_transactions_90d  COUNTER,
    chargeback_count_90d    COUNTER,
    history_risk_score      DECIMAL,        -- atualizado pelo Spark job
    usual_countries         SET<TEXT>,
    usual_merchant_categories SET<TEXT>,
    known_devices           SET<TEXT>,
    first_seen              TIMESTAMP,
    last_seen               TIMESTAMP,
    last_updated            TIMESTAMP
);

-- Counter table separada (Cassandra exige counters em tabela própria)
CREATE TABLE IF NOT EXISTS customer_counters (
    customer_id             TEXT PRIMARY KEY,
    total_transactions      COUNTER,
    total_chargebacks       COUNTER,
    total_approvals         COUNTER,
    total_rejections        COUNTER
);

-- ──────────────────────────────────────────────
-- Índices secundários para queries analíticas
-- ──────────────────────────────────────────────

-- Buscar transações por merchant (ALLOW FILTERING somente em dev)
CREATE INDEX IF NOT EXISTS idx_merchant
ON customer_transactions (merchant_id);

-- ──────────────────────────────────────────────
-- Materialized View: transações por status (para monitoramento)
-- ──────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_by_status AS
    SELECT customer_id, bucket, status, event_time, transaction_id,
           amount, currency, risk_score
    FROM customer_transactions
    WHERE customer_id IS NOT NULL
      AND bucket IS NOT NULL
      AND status IS NOT NULL
      AND event_time IS NOT NULL
      AND transaction_id IS NOT NULL
    PRIMARY KEY ((status, bucket), event_time, customer_id, transaction_id)
    WITH CLUSTERING ORDER BY (event_time DESC);
