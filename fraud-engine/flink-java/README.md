# fraud-detection (Apache Flink — Java)

Job Flink em Java 11 para detecção de fraude em tempo real.

## Versão

| Componente | Versão |
|---|---|
| Apache Flink | **1.18.1 (LTS)** |
| Java | 11 |
| Kafka Connector | 3.1.0-1.18 |
| DataStax Cassandra Driver | 4.17.0 |
| Jedis (Redis) | 4.4.3 |

## Build

```bash
cd fraud-engine/flink-java

# Compilar e gerar fat-JAR
mvn clean package -DskipTests

# Compilar com testes
mvn clean package
```

O fat-JAR é gerado em:
```
target/fraud-detection-1.0.0-with-dependencies.jar
```

## Testes

```bash
mvn test
```

## Submeter ao Flink

```bash
# Via REST API (Flink Web UI em localhost:8081)
curl -X POST http://localhost:8081/jars/upload \
  -H "Expect:" \
  -F "jarfile=@target/fraud-detection-1.0.0-with-dependencies.jar"

# Via CLI
flink run \
  -m localhost:8081 \
  target/fraud-detection-1.0.0-with-dependencies.jar
```

## Variáveis de Ambiente

Todas as configurações são injetadas via env vars (ver `.env.example` na raiz):

```
KAFKA_BOOTSTRAP_SERVERS     localhost:9092
KAFKA_TOPIC_TRANSACTIONS    transactions.raw
KAFKA_TOPIC_DECISIONS       transactions.decisions
KAFKA_GROUP_ID              risk-flow-flink
REDIS_HOST                  localhost
REDIS_PORT                  6379
CASSANDRA_HOSTS             localhost
CASSANDRA_PORT              9042
CASSANDRA_KEYSPACE          risk_flow
FLINK_PARALLELISM           4
FLINK_CHECKPOINT_INTERVAL_MS 30000

# Thresholds das regras (todos opcionais — têm defaults)
RULE_HIGH_AMOUNT_REVIEW     10000
RULE_HIGH_AMOUNT_REJECT     50000
RULE_VELOCITY_1MIN_REVIEW   5
RULE_VELOCITY_1MIN_REJECT   10
RULE_BLOCKED_COUNTRIES      KP,IR,CU,SY
```

## Estrutura

```
src/main/java/com/riskflow/
├── job/
│   └── FraudDetectionJob.java     ← Entrypoint — monta o pipeline Flink
├── models/
│   ├── Transaction.java           ← POJO da transação (Kafka → Flink)
│   ├── CustomerContext.java       ← Contexto Redis + Cassandra
│   ├── EnrichedTransaction.java   ← Transaction + Context
│   └── RiskDecision.java          ← Saída do rule engine
├── config/
│   └── RuleConfig.java            ← Thresholds das regras (env vars)
├── rules/
│   ├── RiskRule.java              ← Interface + RuleResult
│   ├── Rules.java                 ← 10 implementações de regras
│   └── RuleEngine.java            ← Orquestra as regras
├── processors/
│   ├── EnrichmentFunction.java    ← RichMapFunction: Redis + Cassandra
│   ├── WarmMemoryAdapter.java     ← Cassandra DataStax Driver
│   └── DecisionFunction.java      ← RichMapFunction: aplica RuleEngine
└── sinks/
    └── KafkaSchemas.java          ← Serializers/Deserializers Kafka
```

## SLA e Performance

- **Meta**: decisão em < 2 segundos end-to-end
- **Enriquecimento Redis**: ~5-20ms (pipeline Jedis)
- **Enriquecimento Cassandra**: ~10-50ms (prepared statements + LOCAL_ONE)
- **Rule Engine**: < 1ms (avaliação em memória)
- **Budget total**: ~100-200ms normais, máx 2000ms

### Monitoramento de SLA

O job emite um log `⚠️ SLA BREACH` para qualquer transação que ultrapassar 2s.
Esses eventos também aparecem no stdout do task manager para monitoramento.
