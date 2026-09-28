package com.kholodilin.outbox.autoconfigure.smoke;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import com.kholodilin.outbox.queue.redis.RedisOutboxDispatchQueue;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = {OutboxBoot3SmokeApplication.class, OutboxSmokeCachingConfiguration.class},
        properties = {
            "outbox.instance-id=boot3-smoke-redis",
            "outbox.defaults.persistence.table-name=outbox_smoke_redis",
            "outbox.defaults.queue.type=redis",
            "outbox.defaults.queue.redis.key-prefix=outbox:boot3:redis:",
            "spring.cache.type=redis",
            "spring.cache.cache-names=" + OutboxSmokeSupport.CACHE_NAME
        })
@Testcontainers(disabledWithoutDocker = true)
class OutboxBoot3SmokeRedisIT {

    static final String TABLE = "outbox_smoke_redis";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        OutboxSmokeSupport.postgres(registry, POSTGRES);
        OutboxSmokeSupport.redis(registry, REDIS);
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
    void payloadObjectAndStringReachSinkWithRedisCache() {
        OutboxSmokeSupport.publishPayloadsAndAwaitSent(outboxService, sink, jdbcTemplate, transactionManager, TABLE);
        assertThat(registry.getRequired("default").queue()).isInstanceOf(RedisOutboxDispatchQueue.class);
        assertThat(cacheManager).isInstanceOf(RedisCacheManager.class);
        Cache cache = cacheManager.getCache(OutboxSmokeSupport.CACHE_NAME);
        assertThat(cache).isNotNull();
        cache.put("k", "v");
        assertThat(cache.get("k", String.class)).isEqualTo("v");
        assertThat(outboxHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
