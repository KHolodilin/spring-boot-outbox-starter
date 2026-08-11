package com.kholodilin.outbox.worker;

import java.time.Duration;
import java.util.List;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PublisherWorkerLifecycleTest {

    @Test
    void startWithoutSinkFails() {
        DefaultOutboxChannel channel = new DefaultOutboxChannel(
                "default", mock(OutboxStore.class), mock(OutboxDispatchQueue.class), null, props(true));
        PublisherWorker worker = new PublisherWorker(channel, "i", OutboxMetrics.noop());
        assertThatThrownBy(worker::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void disabledPublisherDoesNotStart() throws Exception {
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(queue.poll(any())).thenReturn(null);
        DefaultOutboxChannel channel =
                new DefaultOutboxChannel("default", mock(OutboxStore.class), queue, batch -> null, props(false));
        PublisherWorker worker = new PublisherWorker(channel, "i", OutboxMetrics.noop());
        worker.start();
        worker.close();
    }

    @Test
    void reenqueuesWhenClaimEmpty() throws Exception {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(queue.poll(any())).thenReturn(9L);
        when(queue.drain(org.mockito.ArgumentMatchers.anyInt())).thenReturn(List.of());
        when(store.claimByIds(any(), any(), any())).thenReturn(List.of());
        when(store.findReenqueueableIds(any())).thenReturn(List.of(9L));

        DefaultOutboxChannel channel = new DefaultOutboxChannel(
                "default",
                store,
                queue,
                batch -> new com.kholodilin.outbox.model.OutboxPublishResult.AllSucceeded(),
                props(true));
        PublisherWorker worker = new PublisherWorker(channel, "i", OutboxMetrics.noop());
        worker.processOnce();
        org.mockito.Mockito.verify(queue).offer(9L);
    }

    private static OutboxChannelProperties props(boolean publisherEnabled) {
        return new OutboxChannelProperties(
                "default",
                "outbox_events",
                OutboxChannelProperties.SchemaMode.NONE,
                OutboxChannelProperties.QueueType.MEMORY,
                10,
                5,
                Duration.ofMillis(5),
                0.8,
                "outbox:",
                publisherEnabled,
                Duration.ofSeconds(1),
                3,
                false,
                Duration.ofSeconds(10),
                10);
    }
}
