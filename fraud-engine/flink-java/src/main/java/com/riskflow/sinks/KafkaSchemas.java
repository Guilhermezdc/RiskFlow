package com.riskflow.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.riskflow.models.RiskDecision;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Schemas de serialização/deserialização para integração com Kafka.
 */
public final class KafkaSchemas {

    private KafkaSchemas() {}

    // ════════════════════════════════════════════════════════
    // Deserialização: Kafka (bytes) → String JSON
    // ════════════════════════════════════════════════════════

    /**
     * Deserializa mensagens Kafka como String UTF-8 simples.
     * A conversão String → Transaction é feita no {@link com.riskflow.processors.EnrichmentFunction}.
     */
    public static class TransactionDeserializer
            implements KafkaRecordDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record,
                                org.apache.flink.util.Collector<String> out) {
            if (record.value() == null) {
                LOG.warn("Mensagem Kafka com value null — ignorada | offset={} partition={}",
                        record.offset(), record.partition());
                return;
            }
            out.collect(new String(record.value(), StandardCharsets.UTF_8));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    // ════════════════════════════════════════════════════════
    // Serialização: RiskDecision → Kafka (bytes)
    // ════════════════════════════════════════════════════════

    /**
     * Serializa {@link RiskDecision} para JSON UTF-8.
     */
    public static class DecisionSerializer implements SerializationSchema<RiskDecision> {

        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public void open(InitializationContext context) {
            mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }

        @Override
        public byte[] serialize(RiskDecision decision) {
            try {
                return mapper.writeValueAsBytes(decision);
            } catch (Exception e) {
                throw new RuntimeException("Erro ao serializar RiskDecision: " + e.getMessage(), e);
            }
        }
    }
}
