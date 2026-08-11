package com.kholodilin.outbox.jdbc;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class JdbcOutboxStoreIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private JdbcOutboxStore store;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = new JdbcTemplate(ds);
        new OutboxSchemaManager(ds).apply("outbox_events_orders", OutboxChannelProperties.SchemaMode.CREATE);
        store = new JdbcOutboxStore(
                jdbc, "outbox_events_orders", "orders", JsonMapper.builder().build());
    }

    @Test
    void insertClaimMarkSentAndRecovery() {
        long id = store.insert(
                new OutboxInsert("ORDER_CREATED", "agg-1", "part-1", "{\"ok\":true}", Map.of("k", "v"), "00-trace"));

        List<OutboxRecord> claimed =
                store.claimByIds(List.of(id), "pod-1", Instant.now().plusSeconds(30));
        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().eventType()).isEqualTo("ORDER_CREATED");
        assertThat(claimed.getFirst().headers()).containsEntry("k", "v");

        store.markSent(List.of(id), Instant.now());
        Integer status = jdbc.queryForObject("SELECT status FROM outbox_events_orders WHERE id = ?", Integer.class, id);
        assertThat(status).isEqualTo(OutboxStatus.SENT.getCode());

        long failedId = store.insert(new OutboxInsert("ORDER_FAILED", "agg-2", "part-2", "{}", Map.of(), null));
        store.markFailed(failedId, 1, OutboxStatus.FAILED);

        List<Long> recovered =
                store.claimRecoverableIds(10, "pod-2", Instant.now().plusSeconds(30));
        assertThat(recovered).contains(failedId);
        store.clearLease(recovered);
        assertThat(store.findReenqueueableIds(recovered)).contains(failedId);
    }

    @Test
    void secondTableIsIsolated() {
        DataSource ds = dataSource();
        new OutboxSchemaManager(ds).apply("outbox_events_webhooks", OutboxChannelProperties.SchemaMode.CREATE);
        JdbcOutboxStore webhooks = new JdbcOutboxStore(
                new JdbcTemplate(ds),
                "outbox_events_webhooks",
                "webhooks",
                JsonMapper.builder().build());

        long ordersId = store.insert(new OutboxInsert("A", "1", "p", "{}", Map.of(), null));
        long webhooksId = webhooks.insert(new OutboxInsert("B", "1", "p", "{}", Map.of(), null));

        assertThat(store.findByIds(List.of(ordersId))).hasSize(1);
        assertThat(store.findByIds(List.of(webhooksId))).isEmpty();
        assertThat(webhooks.findByIds(List.of(webhooksId))).hasSize(1);
    }

    private static DataSource dataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }
}
