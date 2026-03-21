package com.riskflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Decisão de risco produzida pelo Flink Rule Engine.
 * Publicada no tópico Kafka {@code transactions.decisions}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RiskDecision implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        APPROVE, REJECT, MANUAL_REVIEW
    }

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("currency")
    private String currency;

    /** Decisão final: APPROVE | REJECT | MANUAL_REVIEW */
    @JsonProperty("status")
    private String status;

    /** Score de risco agregado: 0.0 (baixo risco) → 1.0 (alto risco) */
    @JsonProperty("risk_score")
    private double riskScore;

    /** Nomes das regras que foram disparadas */
    @JsonProperty("triggered_rules")
    private List<String> triggeredRules;

    @JsonProperty("transaction_timestamp")
    private String transactionTimestamp;

    @JsonProperty("decision_timestamp")
    @Builder.Default
    private String decisionTimestamp = Instant.now().toString();

    /** Latência total desde o event_time_ms até a decisão */
    @JsonProperty("processing_latency_ms")
    private long processingLatencyMs;
}
