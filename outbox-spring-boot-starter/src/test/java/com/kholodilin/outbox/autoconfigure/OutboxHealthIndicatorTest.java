package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.MapOutboxChannelRegistry;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxHealthIndicatorTest {

    @Test
    void upWhenBelowThreshold() {
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(queue.pressure()).thenReturn(0.1);
        when(queue.size()).thenReturn(1);
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(new MapOutboxChannelRegistry(java.util.Map.of(
                "default", new DefaultOutboxChannel("default", mock(OutboxStore.class), queue, null, props(0.8)))));
        Health health = indicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void pressureStatusWhenAboveThreshold() {
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        when(queue.pressure()).thenReturn(0.95);
        when(queue.size()).thenReturn(95);
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(new MapOutboxChannelRegistry(java.util.Map.of(
                "default", new DefaultOutboxChannel("default", mock(OutboxStore.class), queue, null, props(0.8)))));
        Health health = indicator.health();
        assertThat(health.getStatus().getCode()).isEqualTo("OUTBOX_PRESSURE");
        assertThat(health.getDetails()).containsKey("channels");
    }

    private static OutboxChannelProperties props(double threshold) {
        return new OutboxChannelProperties(
                "default",
                "outbox_events",
                OutboxChannelProperties.SchemaMode.NONE,
                OutboxChannelProperties.QueueType.MEMORY,
                100,
                10,
                Duration.ofMillis(10),
                threshold,
                "outbox:",
                true,
                Duration.ofSeconds(1),
                1,
                true,
                Duration.ofSeconds(1),
                1);
    }
}
