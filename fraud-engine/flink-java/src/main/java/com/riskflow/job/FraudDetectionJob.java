package com.riskflow.job;

import com.riskflow.models.EnrichedTransaction;
import com.riskflow.models.RiskDecision;
import com.riskflow.processors.DecisionFunction;
import com.riskflow.processors.EnrichmentFunction;
import com.riskflow.sinks.KafkaSchemas;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Job principal do Apache Flink para detecção de fraude em tempo real.
 *
 * <p><b>Pipeline:</b></p>
 * <pre>
 * Kafka (transactions.raw)
 *   → EnrichmentFunction  (Redis hot + Cassandra warm)
 *   → DecisionFunction    (Rule Engine)
 *   → Kafka (transactions.decisions)
 * </pre>
 *
 * <p><b>SLA:</b> decisão em &lt; 2 segundos end-to-end.</p>
 *
 * <p><b>Build e execução:</b></p>
 * <pre>
 *   mvn clean package -DskipTests
 *   flink run target/fraud-detection-1.0.0-with-dependencies.jar
 * </pre>
 */
public class FraudDetectionJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionJob.class);
    private static final String JOB_NAME = "RiskFlow — Fraud Detection";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = createEnvironment();

        buildPipeline(env);

        LOG.info("Submetendo job: {}", JOB_NAME);
        env.execute(JOB_NAME);
    }

    // ════════════════════════════════════════════════════════
    // Environment setup
    // ════════════════════════════════════════════════════════

    static StreamExecutionEnvironment createEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Paralelismo
        int parallelism = Integer.parseInt(
                System.getenv().getOrDefault("FLINK_PARALLELISM", "4"));
        env.setParallelism(parallelism);

        // Checkpointing — garante exactly-once + fault tolerance
        long checkpointInterval = Long.parseLong(
                System.getenv().getOrDefault("FLINK_CHECKPOINT_INTERVAL_MS", "30000"));
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Permite continuar job após falha em checkpoint
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        return env;
    }

    // ════════════════════════════════════════════════════════
    // Pipeline DAG
    // ════════════════════════════════════════════════════════

    static void buildPipeline(StreamExecutionEnvironment env) {
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic  = System.getenv().getOrDefault(
                "KAFKA_TOPIC_TRANSACTIONS", "transactions.raw");
        String outputTopic = System.getenv().getOrDefault(
                "KAFKA_TOPIC_DECISIONS", "transactions.decisions");
        String groupId = System.getenv().getOrDefault(
                "KAFKA_GROUP_ID", "risk-flow-flink");

        LOG.info("Configurando pipeline | bootstrap={} | input={} | output={}",
                bootstrapServers, inputTopic, outputTopic);

        // ── Source: Kafka ─────────────────────────────────
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaSchemas.TransactionDeserializer())
                .setProperties(kafkaConsumerProperties())
                .build();

        // ── Sink: Kafka ───────────────────────────────────
        KafkaSink<RiskDecision> kafkaSink = KafkaSink.<RiskDecision>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<RiskDecision>builder()
                                .setTopic(outputTopic)
                                // Usa customer_id como key para manter ordem por cliente
                                .setKeySerializationSchema(
                                        decision -> decision.getCustomerId() != null
                                                ? decision.getCustomerId().getBytes()
                                                : new byte[0]
                                )
                                .setValueSerializationSchema(new KafkaSchemas.DecisionSerializer())
                                .build()
                )
                .setKafkaProducerConfig(kafkaProducerProperties())
                .build();

        // ── Pipeline ──────────────────────────────────────
        DataStream<String> rawTransactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka: transactions.raw");

        DataStream<EnrichedTransaction> enriched = rawTransactions
                .map(new EnrichmentFunction())
                .name("Enrichment: Redis + Cassandra")
                .uid("enrichment-operator");

        DataStream<RiskDecision> decisions = enriched
                .map(new DecisionFunction())
                .name("Decision: Rule Engine")
                .uid("decision-operator");

        decisions.sinkTo(kafkaSink)
                .name("Kafka: transactions.decisions")
                .uid("kafka-sink");

        // Side output para monitoramento (log de SLA breach)
        decisions
                .filter(d -> d.getProcessingLatencyMs() > 2000)
                .map(d -> String.format("SLA_BREACH tx=%s latency=%dms status=%s",
                        d.getTransactionId(), d.getProcessingLatencyMs(), d.getStatus()))
                .name("SLA Monitor")
                .print();
    }

    // ════════════════════════════════════════════════════════
    // Kafka properties
    // ════════════════════════════════════════════════════════

    private static Properties kafkaConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("auto.offset.reset",    "latest");
        props.setProperty("enable.auto.commit",   "false");   // Flink gerencia offsets
        props.setProperty("max.poll.records",     "500");
        props.setProperty("fetch.min.bytes",      "1");
        props.setProperty("fetch.max.wait.ms",    "500");
        props.setProperty("session.timeout.ms",   "30000");
        return props;
    }

    private static Properties kafkaProducerProperties() {
        Properties props = new Properties();
        props.setProperty("acks",                    "all");
        props.setProperty("retries",                 "5");
        props.setProperty("retry.backoff.ms",        "200");
        props.setProperty("linger.ms",               "5");
        props.setProperty("batch.size",              "65536");
        props.setProperty("compression.type",        "snappy");
        props.setProperty("enable.idempotence",      "true");
        return props;
    }
}
