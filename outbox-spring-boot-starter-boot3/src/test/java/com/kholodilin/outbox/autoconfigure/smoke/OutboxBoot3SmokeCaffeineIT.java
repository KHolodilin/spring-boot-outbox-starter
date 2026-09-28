package com.kholodilin.outbox.autoconfigure.smoke;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import com.kholodilin.outbox.queue.memory.InMemoryOutboxDispatchQueue;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = {OutboxBoot3SmokeApplication.class, OutboxSmokeCachingConfiguration.class},
        properties = {
            "outbox.instance-id=boot3-smoke-caffeine",
            "outbox.defaults.persistence.table-name=outbox_smoke_caffeine",
            "outbox.defaults.queue.type=memory",
            "spring.cache.type=caffeine",
            "spring.cache.cache-names=" + OutboxSmokeSupport.CACHE_NAME,
            "spring.autoconfigure.exclude="
                    + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                    + "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration"
        })
@Testcontainers(disabledWithoutDocker = true)
class OutboxBoot3SmokeCaffeineIT {

    static final String TABLE = "outbox_smoke_caffeine";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        OutboxSmokeSupport.postgres(registry, POSTGRES);
    }

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private RecordingOutboxSink sink;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private OutboxChannelRegistry registry;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    @Qualifier("outboxHealthIndicator") private HealthIndicator outboxHealthIndicator;

    @Test
    void payloadObjectAndStringReachSinkWithCaffeineCache() {
        OutboxSmokeSupport.publishPayloadsAndAwaitSent(outboxService, sink, jdbcTemplate, transactionManager, TABLE);
        assertThat(registry.getRequired("default").queue()).isInstanceOf(InMemoryOutboxDispatchQueue.class);
        assertThat(cacheManager).isInstanceOf(CaffeineCacheManager.class);
        OutboxSmokeSupport.assertNamedCacheRoundTrip(cacheManager);
        assertThat(outboxHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
