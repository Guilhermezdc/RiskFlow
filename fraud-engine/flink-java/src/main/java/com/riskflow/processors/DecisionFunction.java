package com.riskflow.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.riskflow.config.RuleConfig;
import com.riskflow.models.EnrichedTransaction;
import com.riskflow.models.RiskDecision;
import com.riskflow.rules.RuleEngine;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Operador Flink que aplica o Rule Engine e produz a {@link RiskDecision}.
 *
 * <p>SLA: esta função deve completar em &lt; 500ms.
 * O enriquecimento (Redis + Cassandra) consome até ~1s.
 * O budget total para decisão é de 2 segundos.</p>
 *
 * <p>Em caso de erro inesperado, retorna MANUAL_REVIEW como fallback seguro
 * — nunca APPROVE (evita aprovar transações não analisadas).</p>
 */
public class DecisionFunction extends RichMapFunction<EnrichedTransaction, RiskDecision> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DecisionFunction.class);

    // Limite de SLA em ms
    private static final long SLA_LIMIT_MS = 2_000L;

    private transient RuleEngine   ruleEngine;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        RuleConfig config = new RuleConfig();
        ruleEngine = new RuleEngine(config);
        mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
        LOG.info("DecisionFunction inicializada");
    }

    @Override
    public RiskDecision map(EnrichedTransaction enriched) {
        long startMs = System.currentTimeMillis();

        try {
            var tx  = enriched.getTransaction();
            var ctx = enriched.getContext();

            // ── Avalia regras ─────────────────────────────
            RuleEngine.DecisionResult result = ruleEngine.evaluate(tx, ctx);

            long decisionMs   = System.currentTimeMillis();
            long totalLatency = decisionMs - tx.getEventTimeMs();

            // ── Alerta de SLA ─────────────────────────────
            if (totalLatency > SLA_LIMIT_MS) {
                LOG.warn("⚠️ SLA BREACH: {}ms | tx={} | customer={}",
                        totalLatency, tx.getTransactionId(), tx.getCustomerId());
            }

            return RiskDecision.builder()
                    .transactionId(tx.getTransactionId())
                    .customerId(tx.getCustomerId())
                    .amount(tx.getAmount())
                    .currency(tx.getCurrency())
                    .status(result.getStatus())
                    .riskScore(round4(result.getRiskScore()))
                    .triggeredRules(result.getTriggeredRules())
                    .transactionTimestamp(tx.getTimestamp())
                    .decisionTimestamp(Instant.now().toString())
                    .processingLatencyMs(totalLatency)
                    .build();

        } catch (Exception e) {
            LOG.error("Erro na decisão para tx={}: {}",
                    enriched.getTransaction() != null
                            ? enriched.getTransaction().getTransactionId() : "unknown",
                    e.getMessage(), e);

            // Fallback seguro — nunca APPROVE em caso de erro
            String txId = enriched.getTransaction() != null
                    ? enriched.getTransaction().getTransactionId() : "unknown";

            return RiskDecision.builder()
                    .transactionId(txId)
                    .status(RiskDecision.Status.MANUAL_REVIEW.name())
                    .riskScore(0.5)
                    .triggeredRules(List.of("PROCESSING_ERROR"))
                    .decisionTimestamp(Instant.now().toString())
                    .processingLatencyMs(System.currentTimeMillis() - startMs)
                    .build();
        }
    }

    private static double round4(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
