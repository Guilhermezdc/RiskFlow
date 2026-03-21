package com.riskflow.rules;

import com.riskflow.config.RuleConfig;
import com.riskflow.models.CustomerContext;
import com.riskflow.models.RiskDecision;
import com.riskflow.models.Transaction;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Orquestra a avaliação de todas as regras de risco.
 *
 * <p>Avalia as regras em sequência e combina os resultados:</p>
 * <ul>
 *   <li>Se qualquer regra retornar REJECT → status final = REJECT</li>
 *   <li>Se qualquer regra retornar REVIEW (e nenhuma REJECT) → MANUAL_REVIEW</li>
 *   <li>Se todas retornarem PASS → APPROVE</li>
 * </ul>
 *
 * <p>O risk score final é a soma das contribuições individuais, com cap em 1.0.</p>
 *
 * <p>Implementa {@link Serializable} pois é distribuído pelo Flink
 * para os task managers via closure.</p>
 */
public class RuleEngine implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RuleEngine.class);

    private final RuleConfig config;
    private final List<RiskRule> rules;

    public RuleEngine(RuleConfig config) {
        this.config = config;
        this.rules  = buildRuleChain();
        LOG.info("RuleEngine inicializado com {} regras", rules.size());
    }

    /**
     * Avalia todas as regras para a transação enriquecida.
     *
     * @return {@link DecisionResult} com status, score e regras disparadas
     */
    public DecisionResult evaluate(Transaction tx, CustomerContext ctx) {
        List<RiskRule.RuleResult> triggered = new ArrayList<>();

        for (RiskRule rule : rules) {
            try {
                RiskRule.RuleResult result = rule.evaluate(tx, ctx, config);
                if (result.getAction() != RiskRule.Action.PASS) {
                    triggered.add(result);
                    LOG.debug("Regra disparada: {} | ação={} | score={} | tx={}",
                            result.getRuleName(), result.getAction(),
                            result.getScoreContribution(), tx.getTransactionId());
                }
            } catch (Exception e) {
                LOG.error("Erro ao avaliar regra {} para tx {}: {}",
                        rule.getName(), tx.getTransactionId(), e.getMessage(), e);
            }
        }

        // ── Determina ação final ──────────────────────────
        String finalStatus;
        boolean hasReject = triggered.stream()
                .anyMatch(r -> r.getAction() == RiskRule.Action.REJECT);
        boolean hasReview = triggered.stream()
                .anyMatch(r -> r.getAction() == RiskRule.Action.REVIEW);

        if (hasReject) {
            finalStatus = RiskDecision.Status.REJECT.name();
        } else if (hasReview) {
            finalStatus = RiskDecision.Status.MANUAL_REVIEW.name();
        } else {
            finalStatus = RiskDecision.Status.APPROVE.name();
        }

        // ── Risk score agregado (cap em 1.0) ─────────────
        double riskScore = Math.min(
                triggered.stream()
                        .mapToDouble(RiskRule.RuleResult::getScoreContribution)
                        .sum(),
                1.0
        );

        List<String> triggeredNames = triggered.stream()
                .map(RiskRule.RuleResult::getRuleName)
                .collect(Collectors.toList());

        LOG.debug("Decisão: {} | score={} | regras={} | tx={}",
                finalStatus, String.format("%.4f", riskScore),
                triggeredNames, tx.getTransactionId());

        return new DecisionResult(finalStatus, riskScore, triggeredNames);
    }

    // ════════════════════════════════════════════════════════
    // Registro de regras — ordem importa para log/debug
    // ════════════════════════════════════════════════════════

    private List<RiskRule> buildRuleChain() {
        List<RiskRule> chain = new ArrayList<>();
        chain.add(new Rules.BlockedCountryRule());
        chain.add(new Rules.HighChargebackHistoryRule());
        chain.add(new Rules.LowHistoryScoreRule());
        chain.add(new Rules.HighVelocity1MinRule());
        chain.add(new Rules.HighVelocity5MinRule());
        chain.add(new Rules.HighAmountRejectRule());
        chain.add(new Rules.HighAmountReviewRule());
        chain.add(new Rules.NewDeviceHighAmountRule());
        chain.add(new Rules.NewDeviceNewCountryRule());
        chain.add(new Rules.AmountFarAboveAverageRule());
        return chain;
    }

    // ════════════════════════════════════════════════════════
    // Result type
    // ════════════════════════════════════════════════════════

    @Value
    public static class DecisionResult {
        String       status;
        double       riskScore;
        List<String> triggeredRules;
    }
}
