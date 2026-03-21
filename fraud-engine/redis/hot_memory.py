"""
fraud-engine/redis/hot_memory.py

Camada de hot memory usando Redis.
Armazena e recupera o contexto recente do cliente (últimas 24h).

Estratégia de dados:
  - Sorted Set para janelas de tempo (velocidade de transações)
  - Hash para agregados do cliente
  - TTL automático de 24h
"""

import json
import logging
import os
import time
from typing import List, Optional

import redis

logger = logging.getLogger(__name__)


class HotMemoryClient:
    """
    Interface Redis para recuperação rápida do contexto do cliente.
    
    Keys pattern:
      customer:{customer_id}:txs          → Sorted Set (tx_id → timestamp_ms)
      customer:{customer_id}:devices      → Set de device_ids conhecidos
      customer:{customer_id}:aggregates   → Hash com totais pré-computados
    """

    TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", 86400))  # 24h

    def __init__(self, host: str = None, port: int = None, password: str = None):
        self._client = redis.Redis(
            host=host or os.getenv("REDIS_HOST", "localhost"),
            port=port or int(os.getenv("REDIS_PORT", 6379)),
            password=password or os.getenv("REDIS_PASSWORD") or None,
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=1,           # timeout agressivo — não pode travar o Flink
            retry_on_timeout=True,
        )
        logger.info("HotMemoryClient conectado ao Redis")

    # ──────────────────────────────────────────────
    # Escrita — chamada após cada transação aprovada
    # ──────────────────────────────────────────────

    def record_transaction(
        self,
        customer_id: str,
        transaction_id: str,
        amount: float,
        device_id: str,
        timestamp_ms: int,
    ) -> None:
        """Registra uma nova transação no histórico hot do cliente."""
        pipe = self._client.pipeline(transaction=False)

        tx_key = self._tx_key(customer_id)
        agg_key = self._agg_key(customer_id)
        dev_key = self._dev_key(customer_id)

        # Sorted Set: member=transaction_id, score=timestamp_ms
        pipe.zadd(tx_key, {transaction_id: timestamp_ms})
        pipe.expire(tx_key, self.TTL_SECONDS)

        # Device tracking
        pipe.sadd(dev_key, device_id)
        pipe.expire(dev_key, self.TTL_SECONDS)

        # Incrementa agregados
        pipe.hincrbyfloat(agg_key, "total_amount_24h", amount)
        pipe.hincrby(agg_key, "tx_count_24h", 1)
        pipe.hset(agg_key, "last_tx_timestamp", timestamp_ms)
        pipe.expire(agg_key, self.TTL_SECONDS)

        pipe.execute()

    # ──────────────────────────────────────────────
    # Leitura — chamada pelo Flink antes da decisão
    # ──────────────────────────────────────────────

    def get_velocity(self, customer_id: str, now_ms: int = None) -> dict:
        """
        Retorna contagem de transações em janelas de tempo.
        Otimizado para ser chamado no hot path do Flink (< 5ms esperado).
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        tx_key = self._tx_key(customer_id)

        pipe = self._client.pipeline(transaction=False)
        pipe.zcount(tx_key, now_ms - 60_000, now_ms)          # 1 min
        pipe.zcount(tx_key, now_ms - 300_000, now_ms)         # 5 min
        pipe.zcount(tx_key, now_ms - 3_600_000, now_ms)       # 1h
        pipe.zcount(tx_key, now_ms - 86_400_000, now_ms)      # 24h
        results = pipe.execute()

        return {
            "tx_count_last_1min": results[0],
            "tx_count_last_5min": results[1],
            "tx_count_last_1h": results[2],
            "tx_count_last_24h": results[3],
        }

    def get_aggregates(self, customer_id: str) -> dict:
        """Retorna agregados pré-computados do cliente."""
        agg_key = self._agg_key(customer_id)
        data = self._client.hgetall(agg_key)

        return {
            "total_amount_last_24h": float(data.get("total_amount_24h", 0)),
            "tx_count_last_24h": int(data.get("tx_count_24h", 0)),
            "last_transaction_timestamp": data.get("last_tx_timestamp"),
        }

    def get_known_devices(self, customer_id: str) -> List[str]:
        """Retorna dispositivos usados nas últimas 24h."""
        dev_key = self._dev_key(customer_id)
        return list(self._client.smembers(dev_key))

    def get_full_context(self, customer_id: str, now_ms: int = None) -> dict:
        """
        Retorna todo o contexto hot do cliente em uma única chamada otimizada.
        Este é o método chamado pelo Flink no hot path.
        """
        try:
            velocity = self.get_velocity(customer_id, now_ms)
            aggregates = self.get_aggregates(customer_id)
            devices = self.get_known_devices(customer_id)

            return {
                **velocity,
                **aggregates,
                "known_devices": devices,
            }
        except redis.RedisError as e:
            # Redis error não deve derrubar o pipeline — retorna contexto vazio
            logger.warning(f"Redis indisponível para customer {customer_id}: {e}")
            return self._empty_context()

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except redis.RedisError:
            return False

    # ──────────────────────────────────────────────
    # Private
    # ──────────────────────────────────────────────

    @staticmethod
    def _tx_key(customer_id: str) -> str:
        return f"customer:{customer_id}:txs"

    @staticmethod
    def _agg_key(customer_id: str) -> str:
        return f"customer:{customer_id}:aggregates"

    @staticmethod
    def _dev_key(customer_id: str) -> str:
        return f"customer:{customer_id}:devices"

    @staticmethod
    def _empty_context() -> dict:
        return {
            "tx_count_last_1min": 0,
            "tx_count_last_5min": 0,
            "tx_count_last_1h": 0,
            "tx_count_last_24h": 0,
            "total_amount_last_24h": 0.0,
            "last_transaction_timestamp": None,
            "known_devices": [],
        }
