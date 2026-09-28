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
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = OutboxBoot3SmokeApplication.class,
        properties = {
            "outbox.instance-id=boot3-smoke-nocache",
            "outbox.defaults.persistence.table-name=outbox_smoke_nocache",
            "outbox.defaults.queue.type=memory",
            "spring.cache.type=none",
            "spring.autoconfigure.exclude="
                    + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                    + "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration"
        })
@Testcontainers(disabledWithoutDocker = true)
class OutboxBoot3SmokeNoCacheIT {

    static final String TABLE = "outbox_smoke_nocache";

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
    private ApplicationContext applicationContext;

    @Autowired
    @Qualifier("outboxHealthIndicator") private HealthIndicator outboxHealthIndicator;

    @Test
    void payloadObjectAndStringReachSinkWithoutCacheManager() {
        OutboxSmokeSupport.publishPayloadsAndAwaitSent(outboxService, sink, jdbcTemplate, transactionManager, TABLE);
        assertThat(registry.getRequired("default").queue()).isInstanceOf(InMemoryOutboxDispatchQueue.class);
        assertThat(applicationContext.getBeansOfType(CacheManager.class).values())
                .allSatisfy(cm -> assertThat(cm).isInstanceOf(NoOpCacheManager.class));
        assertThat(outboxHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
