"""
fraud-engine/cassandra/warm_memory.py

Camada de warm memory usando Apache Cassandra.
Armazena histórico de transações dos últimos 90 dias e perfil do cliente.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from decimal import Decimal

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout

logger = logging.getLogger(__name__)


class WarmMemoryClient:
    """
    Interface Cassandra para histórico do cliente (90 dias).
    
    Otimizações para o hot path do Flink:
    - Prepared statements (evita parse repetido)
    - Consistency Level LOCAL_ONE (prioriza latência)
    - Connection pool configurado para throughput alto
    """

    KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "risk_flow")

    def __init__(
        self,
        hosts: list = None,
        port: int = None,
        username: str = None,
        password: str = None,
    ):
        hosts = hosts or os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        port = port or int(os.getenv("CASSANDRA_PORT", 9042))

        profile = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_ONE,   # latência < consistência forte
            request_timeout=1.5,                            # SLA: não pode bloquear Flink > 1.5s
        )

        self._cluster = Cluster(
            contact_points=hosts,
            port=port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
        )

        self._session = self._cluster.connect(self.KEYSPACE)
        self._prepare_statements()
        logger.info(f"WarmMemoryClient conectado ao Cassandra: {hosts}")

    # ──────────────────────────────────────────────
    # Prepared Statements (performance crítica)
    # ──────────────────────────────────────────────

    def _prepare_statements(self):
        """Pre-compila todas as queries. Chamado uma vez na inicialização."""

        self._stmt_get_profile = self._session.prepare("""
            SELECT avg_amount, max_amount, history_risk_score,
                   usual_countries, usual_merchant_categories,
                   known_devices, last_seen
            FROM customer_profile
            WHERE customer_id = ?
        """)

        self._stmt_get_recent_txs = self._session.prepare("""
            SELECT transaction_id, amount, status, risk_score,
                   merchant_category, country_code, event_time
            FROM customer_transactions
            WHERE customer_id = ? AND bucket = ?
            ORDER BY event_time DESC
            LIMIT 50
        """)

        self._stmt_count_chargebacks = self._session.prepare("""
            SELECT total_chargebacks, total_transactions
            FROM customer_counters
            WHERE customer_id = ?
        """)

        self._stmt_insert_tx = self._session.prepare("""
            INSERT INTO customer_transactions (
                customer_id, bucket, transaction_id, amount, currency,
                transaction_type, merchant_id, merchant_category,
                merchant_country, device_id, country_code, status,
                risk_score, is_international, event_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        self._stmt_upsert_profile = self._session.prepare("""
            UPDATE customer_profile
            SET last_seen = ?,
                last_updated = ?
            WHERE customer_id = ?
        """)

    # ──────────────────────────────────────────────
    # Leitura — hot path do Flink
    # ──────────────────────────────────────────────

    def get_customer_context(self, customer_id: str) -> dict:
        """
        Recupera o contexto warm do cliente.
        Tolerante a falhas — retorna contexto vazio se Cassandra estiver lento.
        """
        try:
            profile = self._get_profile(customer_id)
            counters = self._get_counters(customer_id)

            # Busca transações do mês atual e anterior
            now = datetime.utcnow()
            current_bucket = now.strftime("%Y-%m")
            prev_bucket = (now - timedelta(days=30)).strftime("%Y-%m")

            txs_current = self._get_recent_txs(customer_id, current_bucket)
            txs_prev = self._get_recent_txs(customer_id, prev_bucket)
            all_txs = txs_current + txs_prev

            return {
                "avg_transaction_amount": float(profile.get("avg_amount") or 0),
                "max_transaction_amount": float(profile.get("max_amount") or 0),
                "usual_merchant_categories": list(profile.get("usual_merchant_categories") or []),
                "usual_countries": list(profile.get("usual_countries") or []),
                "history_risk_score": float(profile.get("history_risk_score") or 1.0),
                "total_transactions_90d": int(counters.get("total_transactions") or 0),
                "chargeback_count_90d": int(counters.get("total_chargebacks") or 0),
                "recent_transactions_count": len(all_txs),
            }

        except (OperationTimedOut, ReadTimeout) as e:
            logger.warning(f"Cassandra timeout para customer {customer_id}: {e}")
            return self._empty_context()

        except Exception as e:
            logger.error(f"Erro Cassandra para customer {customer_id}: {e}")
            return self._empty_context()

    # ──────────────────────────────────────────────
    # Escrita — após decisão do Flink
    # ──────────────────────────────────────────────

    def record_transaction(self, tx: dict, decision: dict) -> None:
        """Persiste a transação e decisão no histórico warm."""
        try:
            now = datetime.utcnow()
            bucket = now.strftime("%Y-%m")

            self._session.execute(
                self._stmt_insert_tx,
                (
                    tx["customer_id"],
                    bucket,
                    tx["transaction_id"],
                    Decimal(str(tx["amount"])),
                    tx["currency"],
                    tx["transaction_type"],
                    tx["merchant_id"],
                    tx["merchant_category"],
                    tx["merchant_country"],
                    tx["device_id"],
                    tx["country_code"],
                    decision["status"],
                    Decimal(str(decision["risk_score"])),
                    tx.get("is_international", False),
                    now,
                ),
            )

            self._session.execute(
                self._stmt_upsert_profile,
                (now, now, tx["customer_id"]),
            )

        except WriteTimeout as e:
            logger.warning(f"Cassandra write timeout para {tx.get('transaction_id')}: {e}")

    # ──────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────

    def _get_profile(self, customer_id: str) -> dict:
        rows = self._session.execute(self._stmt_get_profile, (customer_id,))
        row = rows.one()
        if row:
            return row._asdict()
        return {}

    def _get_counters(self, customer_id: str) -> dict:
        rows = self._session.execute(self._stmt_count_chargebacks, (customer_id,))
        row = rows.one()
        if row:
            return row._asdict()
        return {}

    def _get_recent_txs(self, customer_id: str, bucket: str) -> list:
        rows = self._session.execute(
            self._stmt_get_recent_txs, (customer_id, bucket)
        )
        return list(rows)

    @staticmethod
    def _empty_context() -> dict:
        return {
            "avg_transaction_amount": 0.0,
            "max_transaction_amount": 0.0,
            "usual_merchant_categories": [],
            "usual_countries": [],
            "history_risk_score": 1.0,
            "total_transactions_90d": 0,
            "chargeback_count_90d": 0,
            "recent_transactions_count": 0,
        }

    def close(self):
        self._cluster.shutdown()
        logger.info("WarmMemoryClient encerrado")
