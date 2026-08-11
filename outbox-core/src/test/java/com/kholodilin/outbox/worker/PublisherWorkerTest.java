package com.kholodilin.outbox.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PublisherWorkerTest {

    @Test
    void publishesAndMarksSent() throws Exception {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        OutboxSink sink = mock(OutboxSink.class);

        OutboxRecord record =
                new OutboxRecord("default", 1L, "ORDER_CREATED", "a1", "p1", "{}", Map.of(), null, 0, Instant.now());

        when(queue.poll(any())).thenReturn(1L);
        when(queue.drain(anyInt())).thenReturn(List.of());
        when(store.claimByIds(anyList(), anyString(), any())).thenReturn(List.of(record));
        when(sink.publish(anyList())).thenReturn(new OutboxPublishResult.AllSucceeded());

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, sink, props(5));
        PublisherWorker worker = new PublisherWorker(channel, "inst-1", OutboxMetrics.noop());
        worker.processOnce();

        verify(store).markSent(eq(List.of(1L)), any(Instant.class));
        verify(queue).acknowledge(List.of(1L));
    }

    @Test
    void marksDeadAfterMaxRetries() throws Exception {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        OutboxSink sink = mock(OutboxSink.class);

        OutboxRecord record =
                new OutboxRecord("default", 7L, "ORDER_CREATED", "a1", "p1", "{}", Map.of(), null, 4, Instant.now());

        when(queue.poll(any())).thenReturn(7L);
        when(queue.drain(anyInt())).thenReturn(List.of());
        when(store.claimByIds(anyList(), anyString(), any())).thenReturn(List.of(record));
        when(sink.publish(anyList())).thenReturn(new OutboxPublishResult.AllFailed(new RuntimeException("boom")));

        DefaultOutboxChannel channel = new DefaultOutboxChannel("default", store, queue, sink, props(5));
        PublisherWorker worker = new PublisherWorker(channel, "inst-1", OutboxMetrics.noop());
        worker.processOnce();

        verify(store).markFailed(7L, 5, OutboxStatus.DEAD);
    }

    private static OutboxChannelProperties props(int maxRetries) {
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
                maxRetries,
                true,
                Duration.ofSeconds(10),
                500);
    }
}
