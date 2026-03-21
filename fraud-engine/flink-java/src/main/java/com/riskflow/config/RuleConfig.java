package com.riskflow.config;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configuração do Rule Engine.
 *
 * <p>Todos os thresholds são carregados de variáveis de ambiente,
 * permitindo ajuste sem redeploy do job.</p>
 *
 * <p>Implementa {@link Serializable} pois é distribuído pelo Flink
 * para cada task manager via closure das funções.</p>
 */
@Getter
public class RuleConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RuleConfig.class);

    // ── Valor absoluto ───────────────────────────────
    private final double highAmountReviewThreshold;
    private final double highAmountRejectThreshold;

    // ── Velocidade ───────────────────────────────────
    private final int velocity1MinReview;
    private final int velocity1MinReject;
    private final int velocity5MinReview;

    // ── Score histórico ──────────────────────────────
    private final double historyScoreRejectThreshold;
    private final double historyScoreReviewThreshold;

    // ── Dispositivo novo ─────────────────────────────
    private final double newDeviceHighAmountThreshold;
    private final boolean newDeviceNewCountryReview;

    // ── Desvio da média ──────────────────────────────
    private final double amountAboveAvgMultiplier;

    // ── Chargebacks ──────────────────────────────────
    private final int chargebackRejectCount;

    // ── Países bloqueados ────────────────────────────
    private final List<String> blockedCountries;

    public RuleConfig() {
        this.highAmountReviewThreshold  = getEnvDouble("RULE_HIGH_AMOUNT_REVIEW",  10_000.0);
        this.highAmountRejectThreshold  = getEnvDouble("RULE_HIGH_AMOUNT_REJECT",  50_000.0);

        this.velocity1MinReview         = getEnvInt("RULE_VELOCITY_1MIN_REVIEW",   5);
        this.velocity1MinReject         = getEnvInt("RULE_VELOCITY_1MIN_REJECT",   10);
        this.velocity5MinReview         = getEnvInt("RULE_VELOCITY_5MIN_REVIEW",   15);

        this.historyScoreRejectThreshold = getEnvDouble("RULE_HISTORY_SCORE_REJECT", 0.3);
        this.historyScoreReviewThreshold = getEnvDouble("RULE_HISTORY_SCORE_REVIEW", 0.5);

        this.newDeviceHighAmountThreshold = getEnvDouble("RULE_NEW_DEVICE_AMOUNT",  1_000.0);
        this.newDeviceNewCountryReview    = getEnvBool("RULE_NEW_DEVICE_COUNTRY",   true);

        this.amountAboveAvgMultiplier   = getEnvDouble("RULE_AMOUNT_AVG_MULTIPLIER", 5.0);
        this.chargebackRejectCount      = getEnvInt("RULE_CHARGEBACK_REJECT",        3);

        String countriesEnv = System.getenv().getOrDefault("RULE_BLOCKED_COUNTRIES", "KP,IR,CU,SY");
        this.blockedCountries = Arrays.stream(countriesEnv.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        LOG.info("RuleConfig carregado: highAmountReview={}, velocity1MinReview={}, blockedCountries={}",
                highAmountReviewThreshold, velocity1MinReview, blockedCountries);
    }

    // ── Helpers ──────────────────────────────────────

    private static double getEnvDouble(String key, double defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) return defaultValue;
        try {
            return Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Valor inválido para {}: '{}'. Usando default {}", key, val, defaultValue);
            return defaultValue;
        }
    }

    private static int getEnvInt(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Valor inválido para {}: '{}'. Usando default {}", key, val, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getEnvBool(String key, boolean defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) return defaultValue;
        return "true".equalsIgnoreCase(val.trim());
    }
}
