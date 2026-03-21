package com.riskflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Transação enriquecida com contexto do cliente.
 * É o objeto que o Rule Engine recebe para tomar a decisão.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedTransaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("transaction")
    private Transaction transaction;

    @JsonProperty("context")
    private CustomerContext context;

    @JsonProperty("enriched_at_ms")
    private long enrichedAtMs;
}
