# 🛡️ Risk Flow — Real-Time Transaction Risk Analysis

Sistema de análise de risco em tempo real para transações financeiras, com resposta em < 2 segundos.

## Arquitetura

```
Apache Kafka (Ingestion)
    │
    ├──► Redis (Hot Memory — últimas 24h do cliente)
    │
    └──► Cassandra (Warm Memory — histórico 90 dias)
              │
              ▼
        Apache Flink (Rule Engine — decisão < 2s)
              │
              ├──► APPROVE / REJECT / MANUAL_REVIEW  ──► SNS/SQS
              │
              └──► Apache Spark (Analytics & Parquet)
                        │
                        ├──► S3 (Raw / Parquet)
                        │
                        └──► AWS Redshift
                                  ├── raw
                                  ├── staging
                                  └── mart
```

## Camadas

| Camada | Responsabilidade | Tecnologias |
|---|---|---|
| **Fraud Engine** | Ingestão + Memória + Decisão em tempo real | Kafka, Redis, Cassandra, Flink |
| **Analytics** | Transformação, armazenamento e BI | Spark, S3, Redshift |

## Quick Start

```bash
# Subir toda a infraestrutura local
cd infrastructure/docker
docker compose up -d

# Aguardar serviços (~ 60s)
./scripts/wait-for-services.sh

# Inicializar schemas (Cassandra + Redshift)
./scripts/init-schemas.sh

# Rodar gerador de transações
cd transaction-generator
pip install -r requirements.txt
python generator.py --tps 100 --duration 300
```

## Estrutura de Pastas

```
risk-flow/
├── fraud-engine/           # Camada de risco em tempo real
│   ├── kafka/              # Producers, consumers e config de tópicos
│   ├── redis/              # Client e schemas de hot memory
│   ├── cassandra/          # Client, schemas e queries
│   └── flink/              # Job principal de decisão
│       ├── rules/          # Rule engine (regras de negócio)
│       ├── models/         # DTOs e modelos de dados
│       ├── processors/     # Processadores de stream
│       └── sinks/          # Saídas (SNS, Kafka output)
├── analytics/              # Camada analítica
│   ├── spark/              # Jobs Spark
│   │   ├── jobs/           # Ingestão, transformação, carga
│   │   ├── schemas/        # Schemas Parquet/Avro
│   │   └── utils/          # Helpers S3, logging
│   └── redshift/           # DDLs por camada
│       ├── raw/
│       ├── staging/
│       └── mart/
├── infrastructure/
│   ├── docker/             # docker-compose.yml e configs
│   └── scripts/            # Shell scripts de inicialização
├── transaction-generator/  # Gerador de transações para testes
├── shared/                 # Schemas e utilitários compartilhados
└── docs/                   # Documentação técnica
```

## Variáveis de Ambiente

Copie `.env.example` para `.env` e configure:

```bash
cp .env.example .env
```

## Decisões de Risco

O Flink aplica as seguintes regras (configuráveis em `fraud-engine/flink/rules/`):

| Regra | Threshold | Ação |
|---|---|---|
| Valor alto | > $10.000 | MANUAL_REVIEW |
| Velocidade | > 5 tx / 1 min | MANUAL_REVIEW |
| País bloqueado | Lista negra | REJECT |
| Novo dispositivo + valor alto | Device novo + > $1.000 | MANUAL_REVIEW |
| Score histórico baixo | < 0.3 | REJECT |
| Padrão normal | Demais casos | APPROVE |

## Tecnologias

- **Apache Kafka** 3.6 — Ingestão de eventos
- **Redis** 7.2 — Cache hot (TTL 24h)
- **Apache Cassandra** 4.1 — Warm storage (90 dias)
- **Apache Flink** 1.18 — Stream processing + rule engine
- **Apache Spark** 3.5 — Batch analytics
- **AWS S3** — Data lake (Parquet)
- **AWS Redshift** — Data warehouse (raw/staging/mart)
