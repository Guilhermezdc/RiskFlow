"""
transaction-generator/generator.py

Gerador de transações para testes de carga e validação do pipeline Risk Flow.

Features:
  - Geração realística com distribuições de valor configuráveis
  - Simulação de padrões de fraude (velocidade, países bloqueados, alto valor)
  - Controle de TPS (transações por segundo)
  - Relatório de progresso em tempo real

Uso:
  python generator.py --tps 100 --duration 300
  python generator.py --tps 50 --fraud-rate 0.1 --customers 500
  python generator.py --scenario velocity_attack --customer-id cust_001
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("TransactionGenerator")


# ──────────────────────────────────────────────
# Dados de referência para geração realística
# ──────────────────────────────────────────────

MERCHANT_CATEGORIES = [
    ("5411", "Grocery Stores"),
    ("5812", "Eating Places"),
    ("5912", "Drug Stores"),
    ("5541", "Service Stations"),
    ("5311", "Department Stores"),
    ("7011", "Hotels"),
    ("4111", "Commuter Transport"),
    ("5999", "Misc Retail"),
    ("6011", "ATM Cash"),
]

SAFE_COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "MX", "ES"]
RISKY_COUNTRIES = ["NG", "RU", "UA", "VN"]
BLOCKED_COUNTRIES = ["KP", "IR", "CU", "SY"]

DEVICE_TYPES = ["mobile", "desktop", "pos", "atm"]
CHANNELS = ["online", "in-store", "mobile-app"]
CURRENCIES = ["USD", "USD", "USD", "BRL", "EUR"]  # USD mais frequente


# ──────────────────────────────────────────────
# Gerador de transação individual
# ──────────────────────────────────────────────

def generate_normal_transaction(
    customer_id: str,
    device_pool: List[str],
    usual_country: str,
) -> dict:
    """Gera uma transação normal (sem padrão de fraude)."""
    mcc, merchant_name = random.choice(MERCHANT_CATEGORIES)
    device_id = random.choice(device_pool)

    # Distribuição de valor log-normal (realista para compras do dia-a-dia)
    amount = round(abs(random.lognormvariate(4.0, 1.2)), 2)  # média ~$80
    amount = min(amount, 9500)  # cap abaixo de $10k para transações normais

    now = datetime.utcnow()

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_id": f"acc_{customer_id}",
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choices(
            ["PURCHASE", "WITHDRAWAL", "TRANSFER"],
            weights=[80, 15, 5],
        )[0],
        "merchant_id": f"merch_{random.randint(1000, 9999)}",
        "merchant_category": mcc,
        "merchant_country": usual_country,
        "device_id": device_id,
        "device_type": random.choice(DEVICE_TYPES),
        "ip_address": f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
        "country_code": usual_country,
        "is_international": False,
        "is_new_device": False,
        "channel": random.choice(CHANNELS),
        "timestamp": now.isoformat() + "Z",
        "event_time_ms": int(now.timestamp() * 1000),
    }


def generate_fraud_transaction(
    customer_id: str,
    device_pool: List[str],
    usual_country: str,
    scenario: str = "random",
) -> dict:
    """Gera uma transação com padrão de fraude."""
    base = generate_normal_transaction(customer_id, device_pool, usual_country)

    scenarios = {
        "high_amount": lambda tx: tx.update({
            "amount": round(random.uniform(10001, 75000), 2),
        }),
        "blocked_country": lambda tx: tx.update({
            "country_code": random.choice(BLOCKED_COUNTRIES),
            "merchant_country": random.choice(BLOCKED_COUNTRIES),
            "is_international": True,
        }),
        "new_device": lambda tx: tx.update({
            "device_id": f"new_device_{uuid.uuid4().hex[:8]}",
            "is_new_device": True,
            "amount": round(random.uniform(1500, 5000), 2),
        }),
        "risky_country": lambda tx: tx.update({
            "country_code": random.choice(RISKY_COUNTRIES),
            "is_international": True,
            "is_new_device": True,
            "device_id": f"new_device_{uuid.uuid4().hex[:8]}",
        }),
    }

    chosen = scenario if scenario in scenarios else random.choice(list(scenarios.keys()))
    scenarios[chosen](base)
    base["_fraud_scenario"] = chosen  # meta campo apenas para análise de testes

    return base


# ──────────────────────────────────────────────
# Simulação de velocidade (card testing)
# ──────────────────────────────────────────────

def generate_velocity_burst(customer_id: str, count: int = 12) -> List[dict]:
    """Simula um ataque de card testing — múltiplas transações em < 1 minuto."""
    device_pool = [f"device_{uuid.uuid4().hex[:8]}"]
    txs = []
    for i in range(count):
        tx = generate_normal_transaction(customer_id, device_pool, "US")
        tx["amount"] = round(random.uniform(1, 10), 2)  # valores pequenos (card testing)
        tx["_fraud_scenario"] = "velocity_attack"
        txs.append(tx)
    return txs


# ──────────────────────────────────────────────
# Pool de clientes simulados
# ──────────────────────────────────────────────

@dataclass
class SimulatedCustomer:
    customer_id: str
    usual_country: str
    device_pool: List[str]
    is_high_risk: bool = False


def create_customer_pool(size: int) -> List[SimulatedCustomer]:
    """Cria um pool de clientes com perfis variados."""
    customers = []
    for i in range(size):
        country = random.choice(SAFE_COUNTRIES)
        devices = [f"device_{uuid.uuid4().hex[:8]}" for _ in range(random.randint(1, 3))]
        is_high_risk = random.random() < 0.05  # 5% de clientes de alto risco
        customers.append(
            SimulatedCustomer(
                customer_id=f"cust_{i:05d}",
                usual_country=country,
                device_pool=devices,
                is_high_risk=is_high_risk,
            )
        )
    return customers


# ──────────────────────────────────────────────
# Kafka Producer wrapper simples
# ──────────────────────────────────────────────

class GeneratorProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 10,
            "batch.size": 65536,
            "compression.type": "snappy",
        })
        self.topic = topic
        self.sent = 0
        self.errors = 0

    def send(self, tx: dict):
        self._producer.produce(
            topic=self.topic,
            key=tx["customer_id"].encode(),
            value=json.dumps(tx).encode(),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def _on_delivery(self, err, msg):
        if err:
            self.errors += 1
        else:
            self.sent += 1

    def flush(self):
        self._producer.flush(30)


# ──────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────

def run(args):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions.raw")

    logger.info(f"🚀 Iniciando gerador | TPS={args.tps} | duração={args.duration}s")
    logger.info(f"   Kafka: {bootstrap_servers} → {topic}")
    logger.info(f"   Clientes: {args.customers} | Fraude: {args.fraud_rate*100:.0f}%")

    producer = GeneratorProducer(bootstrap_servers, topic)
    customers = create_customer_pool(args.customers)

    interval = 1.0 / args.tps
    start_time = time.time()
    total_generated = 0
    fraud_generated = 0
    last_report = start_time

    try:
        while True:
            elapsed = time.time() - start_time
            if args.duration > 0 and elapsed >= args.duration:
                break

            loop_start = time.time()
            customer = random.choice(customers)

            # Determina se é fraude
            is_fraud = random.random() < args.fraud_rate or customer.is_high_risk

            # Ocasionalmente injeta burst de velocidade
            if args.velocity_bursts and random.random() < 0.001:
                burst_txs = generate_velocity_burst(customer.customer_id)
                for tx in burst_txs:
                    producer.send(tx)
                total_generated += len(burst_txs)
                fraud_generated += len(burst_txs)
                logger.info(f"💥 Velocity burst injetado para {customer.customer_id}")
                continue

            if is_fraud:
                tx = generate_fraud_transaction(
                    customer.customer_id,
                    customer.device_pool,
                    customer.usual_country,
                )
                fraud_generated += 1
            else:
                tx = generate_normal_transaction(
                    customer.customer_id,
                    customer.device_pool,
                    customer.usual_country,
                )

            producer.send(tx)
            total_generated += 1

            # Relatório a cada 10s
            now = time.time()
            if now - last_report >= 10:
                actual_tps = total_generated / max(now - start_time, 1)
                fraud_pct = (fraud_generated / max(total_generated, 1)) * 100
                logger.info(
                    f"📊 {total_generated:,} txs | "
                    f"TPS real: {actual_tps:.1f} | "
                    f"Fraudes: {fraud_generated:,} ({fraud_pct:.1f}%) | "
                    f"Erros Kafka: {producer.errors}"
                )
                last_report = now

            # Rate limiting
            elapsed_loop = time.time() - loop_start
            sleep_time = interval - elapsed_loop
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")

    finally:
        producer.flush()
        total_time = time.time() - start_time
        logger.info(
            f"\n✅ Gerador finalizado\n"
            f"   Total enviado : {total_generated:,} transações\n"
            f"   Fraudes       : {fraud_generated:,} ({fraud_generated/max(total_generated,1)*100:.1f}%)\n"
            f"   Tempo total   : {total_time:.1f}s\n"
            f"   TPS médio     : {total_generated/max(total_time,1):.1f}\n"
            f"   Erros Kafka   : {producer.errors}"
        )


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Risk Flow — Transaction Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python generator.py --tps 100 --duration 300
  python generator.py --tps 50 --fraud-rate 0.1 --customers 500
  python generator.py --tps 200 --velocity-bursts
        """,
    )
    parser.add_argument("--tps", type=int, default=50, help="Transações por segundo (default: 50)")
    parser.add_argument("--duration", type=int, default=60, help="Duração em segundos. 0=infinito (default: 60)")
    parser.add_argument("--customers", type=int, default=1000, help="Tamanho do pool de clientes simulados (default: 1000)")
    parser.add_argument("--fraud-rate", type=float, default=0.05, help="Taxa de fraude 0.0-1.0 (default: 0.05)")
    parser.add_argument("--velocity-bursts", action="store_true", help="Injeta bursts de velocidade aleatórios")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
