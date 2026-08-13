package com.kholodilin.outbox.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecoveryWorkerTest {

    @Test
    void disabledRecoveryIsNoOp() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, null, props(false));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", OutboxMetrics.noop());

        assertThat(worker.recover()).isZero();
        verify(store, never()).claimRecoverableIds(anyInt(), anyString(), any());
    }

    @Test
    void claimsClearsLeaseAndOffers() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        OutboxSink sink = mock(OutboxSink.class);

        when(store.claimRecoverableIds(anyInt(), anyString(), any())).thenReturn(List.of(1L, 2L));
        when(store.findByIds(anyList())).thenReturn(List.of(record(1L, "A"), record(2L, "B")));
        when(queue.offer(1L)).thenReturn(true);
        when(queue.offer(2L)).thenReturn(true);

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, sink, props(true));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", OutboxMetrics.noop());

        assertThat(worker.recover()).isEqualTo(2);
        verify(store).clearLease(List.of(1L, 2L));
        verify(queue).offer(1L);
        verify(queue).offer(2L);
        verify(sink, never()).publish(anyList());
    }

    @Test
    void emptyClaimReturnsZero() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(store.claimRecoverableIds(anyInt(), anyString(), any())).thenReturn(List.of());

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, null, props(true));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", null);

        assertThat(worker.recover()).isZero();
        verify(store, never()).clearLease(anyList());
    }

    @Test
    void countsOnlySuccessfulOffers() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(store.claimRecoverableIds(anyInt(), anyString(), any())).thenReturn(List.of(1L, 2L));
        when(store.findByIds(anyList())).thenReturn(List.of(record(1L, "A"), record(2L, "B")));
        when(queue.offer(1L)).thenReturn(true);
        when(queue.offer(2L)).thenReturn(false);

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, null, props(true));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", OutboxMetrics.noop());

        assertThat(worker.recover()).isEqualTo(1);
    }

    @Test
    void startAndCloseAreIdempotent() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(store.claimRecoverableIds(anyInt(), anyString(), any())).thenReturn(List.of());

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, null, props(true));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", OutboxMetrics.noop());
        worker.start();
        worker.start();
        worker.close();
        worker.close();
    }

    @Test
    void startIsNoOpWhenRecoveryDisabled() {
        DefaultOutboxChannel channel = new DefaultOutboxChannel(
                "default", mock(OutboxStore.class), mock(OutboxDispatchQueue.class), null, props(false));
        RecoveryWorker worker = new RecoveryWorker(channel, "inst", OutboxMetrics.noop());
        worker.start();
        worker.close();
    }

    private static OutboxRecord record(long id, String type) {
        return new OutboxRecord("default", id, type, "a", "p", "{}", Map.of(), null, 0, Instant.now());
    }

    private static OutboxChannelProperties props(boolean recoveryEnabled) {
        return new OutboxChannelProperties(
                "default",
                "outbox_events",
                OutboxChannelProperties.SchemaMode.NONE,
                OutboxChannelProperties.QueueType.MEMORY,
                100,
                10,
                Duration.ofMillis(10),
                0.8,
                "outbox:",
                true,
                Duration.ofSeconds(30),
                5,
                recoveryEnabled,
                Duration.ofSeconds(10),
                500);
    }
}
