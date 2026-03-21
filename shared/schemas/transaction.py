"""
shared/schemas/transaction.py

Schema canônico de transação — compartilhado por todas as camadas do Risk Flow.
Baseado em campos PCI-DSS relevantes para análise de risco.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional
import json
import uuid


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class TransactionType(str, Enum):
    PURCHASE = "PURCHASE"
    WITHDRAWAL = "WITHDRAWAL"
    TRANSFER = "TRANSFER"
    REFUND = "REFUND"


class Currency(str, Enum):
    USD = "USD"
    BRL = "BRL"
    EUR = "EUR"
    GBP = "GBP"


class DecisionStatus(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class RejectionReason(str, Enum):
    BLOCKED_COUNTRY = "BLOCKED_COUNTRY"
    LOW_HISTORY_SCORE = "LOW_HISTORY_SCORE"
    HIGH_VELOCITY = "HIGH_VELOCITY"
    HIGH_AMOUNT = "HIGH_AMOUNT"
    NEW_DEVICE_HIGH_AMOUNT = "NEW_DEVICE_HIGH_AMOUNT"
    RULE_ENGINE_REJECT = "RULE_ENGINE_REJECT"


# ─────────────────────────────────────────────
# Core Transaction
# ─────────────────────────────────────────────

@dataclass
class Transaction:
    """
    Representa uma transação financeira.
    Este objeto é o evento central que flui por todo o pipeline.
    """

    # Identidade
    transaction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str = ""
    account_id: str = ""

    # Valor
    amount: float = 0.0
    currency: str = Currency.USD

    # Tipo e contexto
    transaction_type: str = TransactionType.PURCHASE
    merchant_id: str = ""
    merchant_category: str = ""        # MCC code (ex: "5411" = supermercado)
    merchant_country: str = ""         # ISO 3166-1 alpha-2

    # Dispositivo e localização
    device_id: str = ""
    device_type: str = ""              # mobile | desktop | atm | pos
    ip_address: str = ""
    country_code: str = ""             # País de origem da transação
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Timestamps
    timestamp: str = field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )
    event_time_ms: int = field(
        default_factory=lambda: int(datetime.utcnow().timestamp() * 1000)
    )

    # Metadados
    is_international: bool = False
    is_new_device: bool = False
    channel: str = ""                  # online | in-store | mobile-app

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Transaction":
        return cls(**json.loads(data))

    @classmethod
    def from_dict(cls, data: dict) -> "Transaction":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─────────────────────────────────────────────
# Enriched Transaction (pós Redis + Cassandra)
# ─────────────────────────────────────────────

@dataclass
class CustomerContext:
    """Contexto do cliente recuperado do Redis (hot) e Cassandra (warm)."""

    customer_id: str = ""

    # Hot memory (Redis — últimas 24h)
    tx_count_last_1min: int = 0
    tx_count_last_5min: int = 0
    tx_count_last_1h: int = 0
    tx_count_last_24h: int = 0
    total_amount_last_24h: float = 0.0
    last_transaction_timestamp: Optional[str] = None
    known_devices: list = field(default_factory=list)

    # Warm memory (Cassandra — últimos 90 dias)
    avg_transaction_amount: float = 0.0
    max_transaction_amount: float = 0.0
    usual_merchant_categories: list = field(default_factory=list)
    usual_countries: list = field(default_factory=list)
    history_risk_score: float = 1.0    # 0.0 (alto risco) a 1.0 (baixo risco)
    total_transactions_90d: int = 0
    chargeback_count_90d: int = 0


@dataclass
class EnrichedTransaction:
    """
    Transação enriquecida com contexto do cliente.
    É este objeto que o Flink rule engine processa para tomar a decisão.
    """
    transaction: Transaction
    context: CustomerContext
    enriched_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )

    def to_dict(self) -> dict:
        return {
            "transaction": asdict(self.transaction),
            "context": asdict(self.context),
            "enriched_at": self.enriched_at,
        }


# ─────────────────────────────────────────────
# Decision (saída do Flink)
# ─────────────────────────────────────────────

@dataclass
class RiskDecision:
    """Resultado da análise de risco produzida pelo Flink."""

    transaction_id: str = ""
    customer_id: str = ""
    amount: float = 0.0
    currency: str = Currency.USD

    # Decisão
    status: str = DecisionStatus.APPROVE
    risk_score: float = 0.0            # 0.0 (baixo risco) a 1.0 (alto risco)
    triggered_rules: list = field(default_factory=list)
    rejection_reason: Optional[str] = None

    # Timestamps
    transaction_timestamp: str = ""
    decision_timestamp: str = field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )
    processing_latency_ms: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "RiskDecision":
        return cls(**json.loads(data))
