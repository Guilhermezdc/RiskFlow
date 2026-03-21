package com.riskflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Contexto do cliente agregado do Redis (hot) e Cassandra (warm).
 * Utilizado pelo Rule Engine para tomar a decisão de risco.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerContext implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("customer_id")
    private String customerId;

    // ── Hot Memory (Redis — últimas 24h) ────────────
    @JsonProperty("tx_count_last_1min")
    private int txCountLast1Min;

    @JsonProperty("tx_count_last_5min")
    private int txCountLast5Min;

    @JsonProperty("tx_count_last_1h")
    private int txCountLast1h;

    @JsonProperty("tx_count_last_24h")
    private int txCountLast24h;

    @JsonProperty("total_amount_last_24h")
    private double totalAmountLast24h;

    @JsonProperty("last_transaction_timestamp")
    private String lastTransactionTimestamp;

    @JsonProperty("known_devices")
    private List<String> knownDevices;

    // ── Warm Memory (Cassandra — 90 dias) ──────────
    @JsonProperty("avg_transaction_amount")
    private double avgTransactionAmount;

    @JsonProperty("max_transaction_amount")
    private double maxTransactionAmount;

    @JsonProperty("usual_merchant_categories")
    private List<String> usualMerchantCategories;

    @JsonProperty("usual_countries")
    private List<String> usualCountries;

    /**
     * Score histórico: 0.0 = alto risco, 1.0 = baixo risco.
     * Default 1.0 para clientes sem histórico (não punir novos clientes).
     */
    @JsonProperty("history_risk_score")
    @Builder.Default
    private double historyRiskScore = 1.0;

    @JsonProperty("total_transactions_90d")
    private int totalTransactions90d;

    @JsonProperty("chargeback_count_90d")
    private int chargebackCount90d;

    /**
     * Retorna um contexto vazio seguro (cliente desconhecido ou serviço indisponível).
     */
    public static CustomerContext empty(String customerId) {
        return CustomerContext.builder()
                .customerId(customerId)
                .historyRiskScore(1.0)
                .knownDevices(List.of())
                .usualCountries(List.of())
                .usualMerchantCategories(List.of())
                .build();
    }
}
