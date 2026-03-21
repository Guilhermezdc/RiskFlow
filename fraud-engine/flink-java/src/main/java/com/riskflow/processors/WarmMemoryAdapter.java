package com.riskflow.processors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.riskflow.models.CustomerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter para buscar o perfil warm do cliente no Cassandra.
 *
 * <p>Inicializado lazy dentro do {@link EnrichmentFunction#open} para evitar
 * problemas de serialização do Flink com o driver DataStax.</p>
 *
 * <p>Usa prepared statements pré-compilados para máxima performance no hot path.</p>
 */
class WarmMemoryAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(WarmMemoryAdapter.class);

    private CqlSession        session;
    private PreparedStatement stmtGetProfile;
    private PreparedStatement stmtGetCounters;

    WarmMemoryAdapter() {
        try {
            String host     = System.getenv().getOrDefault("CASSANDRA_HOSTS", "localhost");
            int    port     = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_PORT", "9042"));
            String keyspace = System.getenv().getOrDefault("CASSANDRA_KEYSPACE", "risk_flow");
            String user     = System.getenv().getOrDefault("CASSANDRA_USER", "cassandra");
            String password = System.getenv().getOrDefault("CASSANDRA_PASSWORD", "cassandra");

            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(1500))
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(5))
                    .build();

            CqlSessionBuilder builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, port))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace(keyspace)
                    .withConfigLoader(loader);

            if (user != null && !user.isBlank()) {
                builder.withAuthCredentials(user, password);
            }

            session = builder.build();

            // Prepared statements — compilados uma vez, reutilizados em todo o stream
            stmtGetProfile = session.prepare(
                    "SELECT avg_amount, max_amount, history_risk_score, " +
                    "       usual_countries, usual_merchant_categories, known_devices " +
                    "FROM customer_profile WHERE customer_id = ?"
            );

            stmtGetCounters = session.prepare(
                    "SELECT total_chargebacks, total_transactions " +
                    "FROM customer_counters WHERE customer_id = ?"
            );

            LOG.info("WarmMemoryAdapter conectado ao Cassandra: {}:{}", host, port);

        } catch (Exception e) {
            LOG.error("Falha ao conectar ao Cassandra: {}", e.getMessage(), e);
            // session permanece null — enrich() retornará o base sem modificações
        }
    }

    /**
     * Enriquece o contexto com dados warm do Cassandra.
     * Retorna {@code base} inalterado se Cassandra estiver indisponível.
     */
    CustomerContext enrich(String customerId, CustomerContext base) {
        if (session == null) {
            return base;
        }

        try {
            // Perfil
            ResultSet profileRs = session.execute(stmtGetProfile.bind(customerId));
            Row profileRow = profileRs.one();

            // Contadores
            ResultSet countersRs = session.execute(stmtGetCounters.bind(customerId));
            Row countersRow = countersRs.one();

            if (profileRow == null && countersRow == null) {
                return base; // cliente novo sem histórico
            }

            CustomerContext.CustomerContextBuilder builder = base.toBuilder();

            if (profileRow != null) {
                Double avgAmount = profileRow.isNull("avg_amount") ? null
                        : profileRow.getBigDecimal("avg_amount").doubleValue();
                Double maxAmount = profileRow.isNull("max_amount") ? null
                        : profileRow.getBigDecimal("max_amount").doubleValue();
                Double histScore = profileRow.isNull("history_risk_score") ? null
                        : profileRow.getBigDecimal("history_risk_score").doubleValue();

                List<String> countries  = toList(profileRow, "usual_countries");
                List<String> categories = toList(profileRow, "usual_merchant_categories");
                List<String> devices    = toList(profileRow, "known_devices");

                if (avgAmount != null) builder.avgTransactionAmount(avgAmount);
                if (maxAmount != null) builder.maxTransactionAmount(maxAmount);
                if (histScore != null) builder.historyRiskScore(histScore);
                if (!countries.isEmpty())  builder.usualCountries(countries);
                if (!categories.isEmpty()) builder.usualMerchantCategories(categories);
                if (!devices.isEmpty()) {
                    // Merge com a lista hot do Redis (já preenchida)
                    List<String> merged = new ArrayList<>(base.getKnownDevices() != null
                            ? base.getKnownDevices() : List.of());
                    devices.stream().filter(d -> !merged.contains(d)).forEach(merged::add);
                    builder.knownDevices(merged);
                }
            }

            if (countersRow != null) {
                long chargebacks = countersRow.isNull("total_chargebacks") ? 0L
                        : countersRow.getLong("total_chargebacks");
                long totalTxs = countersRow.isNull("total_transactions") ? 0L
                        : countersRow.getLong("total_transactions");

                builder
                        .chargebackCount90d((int) Math.min(chargebacks, Integer.MAX_VALUE))
                        .totalTransactions90d((int) Math.min(totalTxs, Integer.MAX_VALUE));
            }

            return builder.build();

        } catch (Exception e) {
            LOG.warn("Cassandra timeout/erro para customer={}: {}", customerId, e.getMessage());
            return base;
        }
    }

    void close() {
        if (session != null && !session.isClosed()) {
            session.close();
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> toList(Row row, String column) {
        try {
            if (row.isNull(column)) return List.of();
            Object val = row.getObject(column);
            if (val instanceof java.util.Set) {
                return new ArrayList<>((java.util.Set<String>) val);
            }
            return List.of();
        } catch (Exception e) {
            return List.of();
        }
    }
}
