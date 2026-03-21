package com.riskflow.rules;

import com.riskflow.config.RuleConfig;
import com.riskflow.models.CustomerContext;
import com.riskflow.models.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários do Rule Engine.
 * Cada regra é testada de forma isolada para garantir comportamento correto.
 */
class RuleEngineTest {

    private RuleConfig config;
    private RuleEngine engine;

    @BeforeEach
    void setUp() {
        config = new RuleConfig();
        engine = new RuleEngine(config);
    }

    // ════════════════════════════════════════════════════════
    // APPROVE: transação limpa
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Transação normal deve ser APPROVED")
    void shouldApproveNormalTransaction() {
        Transaction tx = buildTx(50.0, "US", "US");
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("APPROVE", result.getStatus());
        assertEquals(0.0, result.getRiskScore());
        assertTrue(result.getTriggeredRules().isEmpty());
    }

    // ════════════════════════════════════════════════════════
    // BLOCKED COUNTRY
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("País bloqueado deve REJECT")
    void shouldRejectBlockedCountry() {
        Transaction tx = buildTx(50.0, "KP", "KP"); // Coreia do Norte
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("REJECT", result.getStatus());
        assertTrue(result.getRiskScore() >= 1.0);
        assertTrue(result.getTriggeredRules().contains("BLOCKED_COUNTRY"));
    }

    @Test
    @DisplayName("País do merchant bloqueado deve REJECT")
    void shouldRejectBlockedMerchantCountry() {
        Transaction tx = Transaction.builder()
                .transactionId("tx-001")
                .customerId("cust-001")
                .amount(50.0)
                .currency("USD")
                .countryCode("US")
                .merchantCountry("IR")  // Irã
                .build();
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("REJECT", result.getStatus());
    }

    // ════════════════════════════════════════════════════════
    // HIGH VELOCITY
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Alta velocidade em 1 minuto deve MANUAL_REVIEW")
    void shouldReviewHighVelocity1Min() {
        Transaction tx = buildTx(50.0, "US", "US");
        CustomerContext ctx = CustomerContext.builder()
                .customerId("cust-001")
                .txCountLast1Min(6)  // acima do threshold de review (5)
                .historyRiskScore(1.0)
                .knownDevices(List.of())
                .usualCountries(List.of())
                .usualMerchantCategories(List.of())
                .build();

        var result = engine.evaluate(tx, ctx);

        assertEquals("MANUAL_REVIEW", result.getStatus());
        assertTrue(result.getTriggeredRules().stream()
                .anyMatch(r -> r.contains("VELOCITY")));
    }

    @Test
    @DisplayName("Velocidade extrema em 1 minuto deve REJECT")
    void shouldRejectExtremeVelocity1Min() {
        Transaction tx = buildTx(50.0, "US", "US");
        CustomerContext ctx = CustomerContext.builder()
                .customerId("cust-001")
                .txCountLast1Min(11)  // acima do threshold de reject (10)
                .historyRiskScore(1.0)
                .knownDevices(List.of())
                .usualCountries(List.of())
                .usualMerchantCategories(List.of())
                .build();

        var result = engine.evaluate(tx, ctx);

        assertEquals("REJECT", result.getStatus());
    }

    // ════════════════════════════════════════════════════════
    // HIGH AMOUNT
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Valor acima do threshold de review deve MANUAL_REVIEW")
    void shouldReviewHighAmount() {
        Transaction tx = buildTx(15_000.0, "US", "US"); // acima de $10k
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("MANUAL_REVIEW", result.getStatus());
        assertTrue(result.getTriggeredRules().contains("HIGH_AMOUNT_REVIEW"));
    }

    @Test
    @DisplayName("Valor acima do threshold de reject deve REJECT")
    void shouldRejectExtremeAmount() {
        Transaction tx = buildTx(60_000.0, "US", "US"); // acima de $50k
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("REJECT", result.getStatus());
        assertTrue(result.getTriggeredRules().contains("HIGH_AMOUNT_REJECT"));
    }

    // ════════════════════════════════════════════════════════
    // NEW DEVICE
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Dispositivo novo com valor alto deve MANUAL_REVIEW")
    void shouldReviewNewDeviceHighAmount() {
        Transaction tx = Transaction.builder()
                .transactionId("tx-001")
                .customerId("cust-001")
                .amount(2_000.0)
                .currency("USD")
                .countryCode("US")
                .merchantCountry("US")
                .deviceId("new-device-xyz")
                .newDevice(true)
                .build();
        CustomerContext ctx = buildCleanContext();

        var result = engine.evaluate(tx, ctx);

        assertEquals("MANUAL_REVIEW", result.getStatus());
        assertTrue(result.getTriggeredRules().contains("NEW_DEVICE_HIGH_AMOUNT"));
    }

    // ════════════════════════════════════════════════════════
    // HISTORY SCORE
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Score histórico muito baixo deve REJECT")
    void shouldRejectLowHistoryScore() {
        Transaction tx = buildTx(100.0, "US", "US");
        CustomerContext ctx = CustomerContext.builder()
                .customerId("cust-001")
                .historyRiskScore(0.2)  // abaixo de 0.3
                .knownDevices(List.of())
                .usualCountries(List.of())
                .usualMerchantCategories(List.of())
                .build();

        var result = engine.evaluate(tx, ctx);

        assertEquals("REJECT", result.getStatus());
        assertTrue(result.getTriggeredRules().contains("LOW_HISTORY_SCORE"));
    }

    @Test
    @DisplayName("Score histórico baixo (mas não crítico) deve MANUAL_REVIEW")
    void shouldReviewMediumHistoryScore() {
        Transaction tx = buildTx(100.0, "US", "US");
        CustomerContext ctx = CustomerContext.builder()
                .customerId("cust-001")
                .historyRiskScore(0.4)  // entre 0.3 e 0.5
                .knownDevices(List.of())
                .usualCountries(List.of())
                .usualMerchantCategories(List.of())
                .build();

        var result = engine.evaluate(tx, ctx);

        assertEquals("MANUAL_REVIEW", result.getStatus());
    }

    // ════════════════════════════════════════════════════════
    // RISK SCORE
    // ════════════════════════════════════════════════════════

    @Test
    @DisplayName("Risk score deve ser limitado a 1.0 mesmo com múltiplas regras")
    void riskScoreShouldBeCappedAt1() {
        // Transação que dispara múltiplas regras
        Transaction tx = Transaction.builder()
                .transactionId("tx-001")
                .customerId("cust-001")
                .amount(15_000.0)
                .currency("USD")
                .countryCode("US")
                .merchantCountry("US")
                .deviceId("new-device")
                .newDevice(true)
                .build();

        CustomerContext ctx = CustomerContext.builder()
                .customerId("cust-001")
                .historyRiskScore(0.4)
                .txCountLast1Min(6)
                .chargebackCount90d(2)
                .avgTransactionAmount(50.0)
                .knownDevices(List.of("known-device"))
                .usualCountries(List.of("US"))
                .usualMerchantCategories(List.of())
                .build();

        var result = engine.evaluate(tx, ctx);

        assertTrue(result.getRiskScore() <= 1.0,
                "Risk score não deve exceder 1.0, mas foi: " + result.getRiskScore());
        assertTrue(result.getTriggeredRules().size() > 1,
                "Deveria ter disparado múltiplas regras");
    }

    // ════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════

    private Transaction buildTx(double amount, String countryCode, String merchantCountry) {
        return Transaction.builder()
                .transactionId("tx-" + System.nanoTime())
                .customerId("cust-001")
                .amount(amount)
                .currency("USD")
                .countryCode(countryCode)
                .merchantCountry(merchantCountry)
                .deviceId("known-device-001")
                .build();
    }

    private CustomerContext buildCleanContext() {
        return CustomerContext.builder()
                .customerId("cust-001")
                .txCountLast1Min(0)
                .txCountLast5Min(0)
                .txCountLast1h(0)
                .txCountLast24h(0)
                .totalAmountLast24h(0.0)
                .historyRiskScore(1.0)
                .chargebackCount90d(0)
                .avgTransactionAmount(100.0)
                .knownDevices(List.of("known-device-001"))
                .usualCountries(List.of("US", "BR"))
                .usualMerchantCategories(List.of("5411", "5812"))
                .build();
    }
}
