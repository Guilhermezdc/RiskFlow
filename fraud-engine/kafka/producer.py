"""
fraud-engine/kafka/producer.py

Kafka producer para publicação de transações no tópico transactions.raw.
Usado tanto pelo gerador de testes quanto por sistemas externos.
"""

import json
import logging
import os
from typing import Optional, Callable

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class TransactionProducer:
    """
    Publica transações no Kafka de forma confiável.
    
    Features:
    - Particionamento por customer_id (garante ordem por cliente)
    - Callback de entrega com retry logic
    - Criação automática de tópico se não existir
    """

    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        extra_config: dict = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions.raw")

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",                        # aguarda todos os replicas
            "retries": 5,
            "retry.backoff.ms": 200,
            "linger.ms": 5,                       # micro-batch para throughput
            "batch.size": 65536,
            "compression.type": "snappy",
            "enable.idempotence": True,            # exactly-once semântics
        }

        if extra_config:
            config.update(extra_config)

        self._producer = Producer(config)
        logger.info(f"TransactionProducer conectado em {self.bootstrap_servers}")

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def publish(
        self,
        transaction_dict: dict,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """
        Publica uma transação no Kafka.
        
        O customer_id é usado como partition key para garantir que todas as
        transações do mesmo cliente vão para a mesma partição (ordem garantida).
        """
        customer_id = transaction_dict.get("customer_id", "")
        transaction_id = transaction_dict.get("transaction_id", "")

        try:
            self._producer.produce(
                topic=self.topic,
                key=customer_id.encode("utf-8"),
                value=json.dumps(transaction_dict).encode("utf-8"),
                headers={
                    "transaction_id": transaction_id,
                    "source": "risk-flow",
                },
                on_delivery=on_delivery or self._default_delivery_callback,
            )
            # Poll para processar callbacks de entrega acumulados
            self._producer.poll(0)

        except BufferError:
            logger.warning("Buffer Kafka cheio — fazendo flush antes de retentar")
            self._producer.flush(timeout=10)
            self.publish(transaction_dict, on_delivery)

        except KafkaException as e:
            logger.error(f"Erro ao publicar transação {transaction_id}: {e}")
            raise

    def flush(self, timeout: float = 30.0) -> int:
        """Aguarda todas as mensagens pendentes serem entregues."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"{remaining} mensagens não entregues após flush")
        return remaining

    def close(self) -> None:
        self.flush()
        logger.info("TransactionProducer encerrado")

    # ──────────────────────────────────────────────
    # Admin
    # ──────────────────────────────────────────────

    def ensure_topic_exists(
        self,
        num_partitions: int = 12,
        replication_factor: int = 1,
    ) -> None:
        """Cria o tópico se ainda não existir."""
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin.list_topics(timeout=10)

        if self.topic not in metadata.topics:
            topic = NewTopic(
                self.topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config={
                    "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 dias
                    "cleanup.policy": "delete",
                },
            )
            futures = admin.create_topics([topic])
            for t, f in futures.items():
                try:
                    f.result()
                    logger.info(f"Tópico '{t}' criado com {num_partitions} partições")
                except Exception as e:
                    logger.error(f"Erro ao criar tópico '{t}': {e}")

    # ──────────────────────────────────────────────
    # Private
    # ──────────────────────────────────────────────

    @staticmethod
    def _default_delivery_callback(err, msg):
        if err:
            logger.error(
                f"Falha na entrega | topic={msg.topic()} "
                f"partition={msg.partition()} error={err}"
            )
        else:
            logger.debug(
                f"Mensagem entregue | topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )
