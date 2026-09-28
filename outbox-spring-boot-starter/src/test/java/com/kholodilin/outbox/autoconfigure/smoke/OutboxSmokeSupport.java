package com.kholodilin.outbox.autoconfigure.smoke;

import java.time.Duration;
import java.util.Map;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.model.OutboxStatus;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

final class OutboxSmokeSupport {

    static final String CACHE_NAME = "outbox-smoke";

    private OutboxSmokeSupport() {}

    static void postgres(DynamicPropertyRegistry registry, PostgreSQLContainer postgres) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("outbox.defaults.persistence.schema.mode", () -> "create");
        registry.add("outbox.defaults.recovery.enabled", () -> "false");
    }

    static GenericContainer<?> redisContainer() {
        return new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379)
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
    }

    static void redis(DynamicPropertyRegistry registry, GenericContainer<?> redis) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.cache.redis.time-to-live", () -> "1h");
    }

    static void assertNamedCacheRoundTrip(CacheManager cacheManager) {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        assertThat(cache).isNotNull();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            cache.put("k", "v");
            Cache.ValueWrapper wrapper = cache.get("k");
            assertThat(wrapper).isNotNull();
            assertThat(wrapper.get()).isEqualTo("v");
        });
    }

    static void publishPayloadsAndAwaitSent(
            OutboxService outboxService,
            RecordingOutboxSink sink,
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            String tableName) {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        tx.executeWithoutResult(status -> {
            outboxService
                    .eventType("OBJECT_PAYLOAD")
                    .aggregateId("1")
                    .partitionKey("p1")
                    .payload(Map.of("id", 1))
                    .append();
            outboxService
                    .eventType("STRING_PAYLOAD")
                    .aggregateId("2")
                    .partitionKey("p1")
                    .payload("{\"raw\":true}")
                    .append();
        });

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(sink.published()).hasSize(2);
            Integer sent = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + tableName + " WHERE status = ?",
                    Integer.class,
                    OutboxStatus.SENT.getCode());
            assertThat(sent).isEqualTo(2);
        });

        String objectPayload = jdbcTemplate.queryForObject(
                "SELECT payload::text FROM " + tableName + " WHERE event_type = ?", String.class, "OBJECT_PAYLOAD");
        assertThat(objectPayload).contains("\"id\"");
        assertThat(objectPayload).contains("1");

        String stringPayload = jdbcTemplate.queryForObject(
                "SELECT payload::text FROM " + tableName + " WHERE event_type = ?", String.class, "STRING_PAYLOAD");
        assertThat(stringPayload).contains("\"raw\"");
        assertThat(stringPayload).contains("true");
    }
}
