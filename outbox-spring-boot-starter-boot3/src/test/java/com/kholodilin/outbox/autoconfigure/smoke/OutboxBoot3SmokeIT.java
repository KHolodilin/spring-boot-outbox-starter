package com.kholodilin.outbox.autoconfigure.smoke;

import java.time.Duration;
import java.util.Map;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.model.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = OutboxBoot3SmokeApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxBoot3SmokeIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("outbox.defaults.persistence.schema.mode", () -> "create");
        registry.add("outbox.defaults.recovery.enabled", () -> "false");
        registry.add("outbox.instance-id", () -> "boot3-smoke");
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
    @Qualifier("outboxHealthIndicator") private HealthIndicator outboxHealthIndicator;

    @Test
    void payloadObjectAndStringReachSinkAndHealthIsUp() {
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
                    "SELECT COUNT(*) FROM outbox_events WHERE status = ?", Integer.class, OutboxStatus.SENT.getCode());
            assertThat(sent).isEqualTo(2);
        });

        String objectPayload = jdbcTemplate.queryForObject(
                "SELECT payload::text FROM outbox_events WHERE event_type = ?", String.class, "OBJECT_PAYLOAD");
        assertThat(objectPayload).contains("\"id\"");
        assertThat(objectPayload).contains("1");

        String stringPayload = jdbcTemplate.queryForObject(
                "SELECT payload::text FROM outbox_events WHERE event_type = ?", String.class, "STRING_PAYLOAD");
        assertThat(stringPayload).contains("\"raw\"");
        assertThat(stringPayload).contains("true");

        assertThat(outboxHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
