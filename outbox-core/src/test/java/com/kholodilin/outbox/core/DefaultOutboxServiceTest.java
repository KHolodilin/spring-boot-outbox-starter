package com.kholodilin.outbox.core;

import java.time.Duration;
import java.util.Map;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.MapOutboxChannelRegistry;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.exception.MissingOutboxTransactionException;
import com.kholodilin.outbox.exception.UnknownOutboxChannelException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultOutboxServiceTest {

    @AfterEach
    void cleanupTx() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void appendRequiresTransaction() {
        DefaultOutboxService service = serviceWithDefault();
        assertThatThrownBy(() -> service.eventType("X")
                        .aggregateId("1")
                        .partitionKey("k")
                        .payload("{}")
                        .append())
                .isInstanceOf(MissingOutboxTransactionException.class);
    }

    @Test
    void unknownChannelFailsFast() {
        DefaultOutboxService service = serviceWithDefault();
        assertThatThrownBy(() -> service.channel("nope").eventType("X"))
                .isInstanceOf(UnknownOutboxChannelException.class);
    }

    @Test
    void appendInsertsAndEnqueuesAfterCommit() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(store.insert(any(OutboxInsert.class))).thenReturn(42L);
        when(queue.offer(42L)).thenReturn(true);

        DefaultOutboxChannel channel =
                new DefaultOutboxChannel("default", store, queue, null, props("default", "outbox_events"));
        DefaultOutboxService service = new DefaultOutboxService(
                new MapOutboxChannelRegistry(Map.of("default", channel)),
                JsonMapper.builder().build(),
                OutboxMetrics.noop());

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);

        long id = service.eventType("ORDER_CREATED")
                .aggregateId("1")
                .partitionKey("c1")
                .payload(Map.of("ok", true))
                .header("correlationId", "corr")
                .append();

        assertThat(id).isEqualTo(42L);
        verify(store).insert(any(OutboxInsert.class));

        TransactionSynchronizationManager.getSynchronizations().forEach(sync -> sync.afterCommit());
        verify(queue).offer(42L);
    }

    private static DefaultOutboxService serviceWithDefault() {
        DefaultOutboxChannel channel = new DefaultOutboxChannel(
                "default",
                mock(OutboxStore.class),
                mock(OutboxDispatchQueue.class),
                null,
                props("default", "outbox_events"));
        return new DefaultOutboxService(
                new MapOutboxChannelRegistry(Map.of("default", channel)),
                JsonMapper.builder().build(),
                OutboxMetrics.noop());
    }

    private static OutboxChannelProperties props(String name, String table) {
        return new OutboxChannelProperties(
                name,
                table,
                OutboxChannelProperties.SchemaMode.VALIDATE,
                OutboxChannelProperties.QueueType.MEMORY,
                100,
                10,
                Duration.ofMillis(50),
                0.8,
                "outbox:",
                true,
                Duration.ofSeconds(30),
                5,
                true,
                Duration.ofSeconds(10),
                500);
    }
}
