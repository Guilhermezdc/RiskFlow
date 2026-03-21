package com.riskflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Representa uma transação financeira recebida do Kafka.
 * Schema canônico compartilhado por todo o pipeline Risk Flow.
 *
 * <p>Implementa {@link Serializable} pois o Flink precisa serializar
 * objetos para checkpointing e comunicação entre operadores.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Identidade ──────────────────────────────────
    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("account_id")
    private String accountId;

    // ── Valor ───────────────────────────────────────
    @JsonProperty("amount")
    private double amount;

    @JsonProperty("currency")
    private String currency;

    // ── Tipo e contexto ─────────────────────────────
    @JsonProperty("transaction_type")
    private String transactionType;

    @JsonProperty("merchant_id")
    private String merchantId;

    @JsonProperty("merchant_category")
    private String merchantCategory;

    @JsonProperty("merchant_country")
    private String merchantCountry;

    // ── Dispositivo e localização ───────────────────
    @JsonProperty("device_id")
    private String deviceId;

    @JsonProperty("device_type")
    private String deviceType;

    @JsonProperty("ip_address")
    private String ipAddress;

    @JsonProperty("country_code")
    private String countryCode;

    @JsonProperty("latitude")
    private Double latitude;

    @JsonProperty("longitude")
    private Double longitude;

    // ── Timestamps ──────────────────────────────────
    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("event_time_ms")
    private long eventTimeMs;

    // ── Flags ───────────────────────────────────────
    @JsonProperty("is_international")
    private boolean international;

    @JsonProperty("is_new_device")
    private boolean newDevice;

    @JsonProperty("channel")
    private String channel;
}
