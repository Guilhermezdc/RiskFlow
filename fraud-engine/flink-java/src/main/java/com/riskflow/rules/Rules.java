package com.riskflow.rules;

import com.riskflow.config.RuleConfig;
import com.riskflow.models.CustomerContext;
import com.riskflow.models.Transaction;

import java.util.List;

/**
 * Implementações de todas as regras de risco do Risk Flow.
 *
 * <p>Cada regra é uma classe interna estática, facilitando
 * testes unitários individuais e registro no {@link RuleEngine}.</p>
 */
public final class Rules {

    private Rules() {}

    // ════════════════════════════════════════════════════════
    // REGRA 1: País bloqueado
    // ════════════════════════════════════════════════════════

    public static class BlockedCountryRule implements RiskRule {
        @Override
        public String getName() { return "BLOCKED_COUNTRY"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            List<String> blocked = config.getBlockedCountries();
            String origin  = upper(tx.getCountryCode());
            String merchant = upper(tx.getMerchantCountry());

            if (blocked.contains(origin) || blocked.contains(merchant)) {
                String country = !origin.isEmpty() ? origin : merchant;
                return RuleResult.reject(getName(), 1.0,
                        "Transação de país bloqueado: " + country);
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 2: Alto volume de chargebacks no histórico
    // ════════════════════════════════════════════════════════

    public static class HighChargebackHistoryRule implements RiskRule {
        @Override
        public String getName() { return "HIGH_CHARGEBACK_HISTORY"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            int chargebacks = ctx.getChargebackCount90d();
            if (chargebacks >= config.getChargebackRejectCount()) {
                return RuleResult.reject(getName(), 0.9,
                        chargebacks + " chargebacks nos últimos 90 dias");
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 3: Score histórico baixo
    // ════════════════════════════════════════════════════════

    public static class LowHistoryScoreRule implements RiskRule {
        @Override
        public String getName() { return "LOW_HISTORY_SCORE"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            double score = ctx.getHistoryRiskScore();

            if (score < config.getHistoryScoreRejectThreshold()) {
                return RuleResult.reject(getName(), 0.85,
                        String.format("Score histórico muito baixo: %.2f", score));
            }
            if (score < config.getHistoryScoreReviewThreshold()) {
                return RuleResult.review(getName() + "_REVIEW", 0.4,
                        String.format("Score histórico baixo: %.2f", score));
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 4: Alta velocidade em 1 minuto (card testing)
    // ════════════════════════════════════════════════════════

    public static class HighVelocity1MinRule implements RiskRule {
        @Override
        public String getName() { return "HIGH_VELOCITY_1MIN"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            int count = ctx.getTxCountLast1Min();

            if (count >= config.getVelocity1MinReject()) {
                return RuleResult.reject(getName() + "_REJECT", 0.95,
                        count + " transações no último minuto (possível card testing)");
            }
            if (count >= config.getVelocity1MinReview()) {
                return RuleResult.review(getName() + "_REVIEW", 0.5,
                        count + " transações no último minuto");
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 5: Alta velocidade em 5 minutos
    // ════════════════════════════════════════════════════════

    public static class HighVelocity5MinRule implements RiskRule {
        @Override
        public String getName() { return "HIGH_VELOCITY_5MIN"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            int count = ctx.getTxCountLast5Min();

            if (count >= config.getVelocity5MinReview()) {
                return RuleResult.review(getName(), 0.45,
                        count + " transações nos últimos 5 minutos");
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 6: Valor extremamente alto — REJECT direto
    // ════════════════════════════════════════════════════════

    public static class HighAmountRejectRule implements RiskRule {
        @Override
        public String getName() { return "HIGH_AMOUNT_REJECT"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            if (tx.getAmount() >= config.getHighAmountRejectThreshold()) {
                return RuleResult.reject(getName(), 0.8,
                        String.format("Valor %.2f acima do limite de rejeição %.2f",
                                tx.getAmount(), config.getHighAmountRejectThreshold()));
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 7: Valor alto — REVIEW
    // ════════════════════════════════════════════════════════

    public static class HighAmountReviewRule implements RiskRule {
        @Override
        public String getName() { return "HIGH_AMOUNT_REVIEW"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            if (tx.getAmount() >= config.getHighAmountReviewThreshold()) {
                return RuleResult.review(getName(), 0.35,
                        String.format("Valor %.2f acima do limite de revisão %.2f",
                                tx.getAmount(), config.getHighAmountReviewThreshold()));
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 8: Dispositivo novo + valor alto
    // ════════════════════════════════════════════════════════

    public static class NewDeviceHighAmountRule implements RiskRule {
        @Override
        public String getName() { return "NEW_DEVICE_HIGH_AMOUNT"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            boolean isNewDevice = isNewDevice(tx, ctx);

            if (isNewDevice && tx.getAmount() >= config.getNewDeviceHighAmountThreshold()) {
                return RuleResult.review(getName(), 0.45,
                        String.format("Dispositivo novo com valor alto: %.2f", tx.getAmount()));
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 9: Dispositivo novo + país desconhecido
    // ════════════════════════════════════════════════════════

    public static class NewDeviceNewCountryRule implements RiskRule {
        @Override
        public String getName() { return "NEW_DEVICE_NEW_COUNTRY"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            if (!config.isNewDeviceNewCountryReview()) {
                return RuleResult.pass(getName());
            }

            boolean isNewDevice = isNewDevice(tx, ctx);
            List<String> usualCountries = ctx.getUsualCountries();
            String country = upper(tx.getCountryCode());

            if (isNewDevice
                    && !usualCountries.isEmpty()
                    && !usualCountries.contains(country)) {
                return RuleResult.review(getName(), 0.4,
                        "Dispositivo novo em país incomum: " + country);
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // REGRA 10: Valor muito acima da média histórica do cliente
    // ════════════════════════════════════════════════════════

    public static class AmountFarAboveAverageRule implements RiskRule {
        @Override
        public String getName() { return "AMOUNT_FAR_ABOVE_AVERAGE"; }

        @Override
        public RuleResult evaluate(Transaction tx, CustomerContext ctx, RuleConfig config) {
            double avg = ctx.getAvgTransactionAmount();
            double multiplier = config.getAmountAboveAvgMultiplier();

            if (avg > 0 && tx.getAmount() > avg * multiplier) {
                double ratio = tx.getAmount() / avg;
                return RuleResult.review(getName(), 0.35,
                        String.format("Valor %.2f é %.1fx acima da média histórica %.2f",
                                tx.getAmount(), ratio, avg));
            }
            return RuleResult.pass(getName());
        }
    }

    // ════════════════════════════════════════════════════════
    // Helpers compartilhados
    // ════════════════════════════════════════════════════════

    private static boolean isNewDevice(Transaction tx, CustomerContext ctx) {
        if (tx.isNewDevice()) return true;
        List<String> known = ctx.getKnownDevices();
        if (known == null || known.isEmpty()) return false;
        return tx.getDeviceId() != null && !known.contains(tx.getDeviceId());
    }

    private static String upper(String s) {
        return s == null ? "" : s.toUpperCase();
    }
}
