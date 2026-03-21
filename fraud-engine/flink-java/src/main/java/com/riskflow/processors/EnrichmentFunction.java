package com.riskflow.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.riskflow.models.CustomerContext;
import com.riskflow.models.EnrichedTransaction;
import com.riskflow.models.Transaction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.time.Duration;
import java.util.*;

/**
 * Operador Flink responsável por enriquecer a transação com o contexto do cliente.
 *
 * <p><b>Hot memory</b> — Redis: janelas de velocidade (1min / 5min / 1h / 24h),
 * dispositivos conhecidos e totais das últimas 24h.</p>
 *
 * <p><b>Warm memory</b> — Cassandra: perfil histórico dos últimos 90 dias
 * (avg amount, score histórico, países habituais, chargebacks).</p>
 *
 * <p>Usa {@link RichMapFunction} para abrir conexões uma única vez por task manager,
 * evitando overhead de reconexão por mensagem — crítico para o SLA de 2 segundos.</p>
 *
 * <p>Tolerante a falhas: se Redis ou Cassandra estiverem lentos, o job continua
 * com um {@link CustomerContext} vazio (safe defaults). Nunca derruba o pipeline.</p>
 */
public class EnrichmentFunction extends RichMapFunction<String, EnrichedTransaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentFunction.class);

    private transient ObjectMapper mapper;
    private transient JedisPool    jedisPool;

    // Cassandra é gerenciado pela WarmMemoryAdapter (lazy init para evitar problemas de serialização)
    private transient WarmMemoryAdapter cassandraAdapter;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        // ── Redis (JedisPool) ──────────────────────────
        String redisHost = System.getenv().getOrDefault("REDIS_HOST",     "localhost");
        int    redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(5);
        poolConfig.setMaxWait(Duration.ofMillis(800));   // timeout agressivo
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestWhileIdle(true);

        String redisPassword = System.getenv("REDIS_PASSWORD");
        if (redisPassword != null && !redisPassword.isBlank()) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000, redisPassword);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000);
        }

        // ── Cassandra ──────────────────────────────────
        cassandraAdapter = new WarmMemoryAdapter();

        LOG.info("EnrichmentFunction aberta: Redis={}:{}", redisHost, redisPort);
    }

    @Override
    public EnrichedTransaction map(String json) throws Exception {
        long start = System.currentTimeMillis();
        Transaction tx = mapper.readValue(json, Transaction.class);

        CustomerContext ctx = CustomerContext.empty(tx.getCustomerId());

        // ── Enriquecimento Redis ───────────────────────
        try {
            ctx = enrichFromRedis(tx, ctx);
        } catch (Exception e) {
            LOG.warn("Redis indisponível para customer={}: {}", tx.getCustomerId(), e.getMessage());
        }

        // ── Enriquecimento Cassandra ───────────────────
        try {
            ctx = cassandraAdapter.enrich(tx.getCustomerId(), ctx);
        } catch (Exception e) {
            LOG.warn("Cassandra indisponível para customer={}: {}", tx.getCustomerId(), e.getMessage());
        }

        // Detecta novo device comparando com a lista hot do Redis
        if (tx.getDeviceId() != null && ctx.getKnownDevices() != null
                && !ctx.getKnownDevices().isEmpty()
                && !ctx.getKnownDevices().contains(tx.getDeviceId())) {
            tx = copyWithNewDevice(tx);
        }

        long latency = System.currentTimeMillis() - start;
        if (latency > 1000) {
            LOG.warn("Enriquecimento lento: {}ms | customer={}", latency, tx.getCustomerId());
        }

        return EnrichedTransaction.builder()
                .transaction(tx)
                .context(ctx)
                .enrichedAtMs(System.currentTimeMillis())
                .build();
    }

    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
        if (cassandraAdapter != null) {
            cassandraAdapter.close();
        }
        LOG.info("EnrichmentFunction encerrada");
    }

    // ── Redis: pipeline para buscar todos os dados em uma roundtrip ──

    private CustomerContext enrichFromRedis(Transaction tx, CustomerContext base) {
        long nowMs = System.currentTimeMillis();
        String customerId = tx.getCustomerId();

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipe = jedis.pipelined();

            // Janelas de velocidade (Sorted Set com score = timestamp_ms)
            String txKey  = "customer:" + customerId + ":txs";
            String aggKey = "customer:" + customerId + ":aggregates";
            String devKey = "customer:" + customerId + ":devices";

            redis.clients.jedis.Response<Long> cnt1m  = pipe.zcount(txKey, nowMs - 60_000,      nowMs);
            redis.clients.jedis.Response<Long> cnt5m  = pipe.zcount(txKey, nowMs - 300_000,     nowMs);
            redis.clients.jedis.Response<Long> cnt1h  = pipe.zcount(txKey, nowMs - 3_600_000,   nowMs);
            redis.clients.jedis.Response<Long> cnt24h = pipe.zcount(txKey, nowMs - 86_400_000,  nowMs);
            redis.clients.jedis.Response<Map<String, String>> agg = pipe.hgetAll(aggKey);
            redis.clients.jedis.Response<Set<String>> devices = pipe.smembers(devKey);

            pipe.sync();

            Map<String, String> aggMap = agg.get();
            double totalAmount24h = aggMap != null
                    ? Double.parseDouble(aggMap.getOrDefault("total_amount_24h", "0"))
                    : 0.0;
            String lastTxTs = aggMap != null ? aggMap.get("last_tx_timestamp") : null;
            Set<String> devSet = devices.get() != null ? devices.get() : Collections.emptySet();

            return base.toBuilder()
                    .txCountLast1Min(cnt1m.get() != null ? cnt1m.get().intValue() : 0)
                    .txCountLast5Min(cnt5m.get() != null ? cnt5m.get().intValue() : 0)
                    .txCountLast1h(cnt1h.get() != null ? cnt1h.get().intValue() : 0)
                    .txCountLast24h(cnt24h.get() != null ? cnt24h.get().intValue() : 0)
                    .totalAmountLast24h(totalAmount24h)
                    .lastTransactionTimestamp(lastTxTs)
                    .knownDevices(new ArrayList<>(devSet))
                    .build();
        }
    }

    // Flink exige objetos imutáveis no stream — cria cópia com flag atualizada
    private Transaction copyWithNewDevice(Transaction tx) {
        return Transaction.builder()
                .transactionId(tx.getTransactionId())
                .customerId(tx.getCustomerId())
                .accountId(tx.getAccountId())
                .amount(tx.getAmount())
                .currency(tx.getCurrency())
                .transactionType(tx.getTransactionType())
                .merchantId(tx.getMerchantId())
                .merchantCategory(tx.getMerchantCategory())
                .merchantCountry(tx.getMerchantCountry())
                .deviceId(tx.getDeviceId())
                .deviceType(tx.getDeviceType())
                .ipAddress(tx.getIpAddress())
                .countryCode(tx.getCountryCode())
                .latitude(tx.getLatitude())
                .longitude(tx.getLongitude())
                .timestamp(tx.getTimestamp())
                .eventTimeMs(tx.getEventTimeMs())
                .international(tx.isInternational())
                .newDevice(true)    // <- detectado
                .channel(tx.getChannel())
                .build();
    }
}
