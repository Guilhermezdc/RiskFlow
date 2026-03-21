package com.riskflow.rules;

import com.riskflow.config.RuleConfig;
import com.riskflow.models.CustomerContext;
import com.riskflow.models.Transaction;
import lombok.Builder;
import lombok.Value;

/**
 * Interface que todas as regras de risco devem implementar.
 *
 * <p>Cada implementação avalia a transação + contexto do cliente
 * e retorna um {@link RuleResult} descrevendo o impacto no risco.</p>
 */
public interface RiskRule {

    /**
     * Avalia a regra e retorna o resultado.
     *
     * @param tx      transação a ser avaliada
     * @param ctx     contexto do cliente (Redis + Cassandra)
     * @param config  thresholds configuráveis
     * @return resultado da avaliação
     */
    RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config);

    /**
     * Nome único da regra (usado em triggered_rules e logs).
     */
    String getName();


    // ════════════════════════════════════════════════════════
    // Inner types
    // ════════════════════════════════════════════════════════

    enum Action {
        /** Regra não disparada — transação segue para próxima regra */
        PASS,
        /** Regra disparada — requer revisão manual */
        REVIEW,
        /** Regra disparada — transação deve ser rejeitada imediatamente */
        REJECT
    }

    /**
     * Resultado imutável de uma avaliação de regra.
     */
    @Value
    @Builder
    class RuleResult {
        String   ruleName;
        Action   action;
        /** Contribuição ao risk score: 0.0 a 1.0 */
        double   scoreContribution;
        String   message;

        public static RuleResult pass(String ruleName) {
            return RuleResult.builder()
                    .ruleName(ruleName)
                    .action(Action.PASS)
                    .scoreContribution(0.0)
                    .message("")
                    .build();
        }

        public static RuleResult review(String ruleName, double score, String message) {
            return RuleResult.builder()
                    .ruleName(ruleName)
                    .action(Action.REVIEW)
                    .scoreContribution(score)
                    .message(message)
                    .build();
        }

        public static RuleResult reject(String ruleName, double score, String message) {
            return RuleResult.builder()
                    .ruleName(ruleName)
                    .action(Action.REJECT)
                    .scoreContribution(score)
                    .message(message)
                    .build();
        }
    }
}
