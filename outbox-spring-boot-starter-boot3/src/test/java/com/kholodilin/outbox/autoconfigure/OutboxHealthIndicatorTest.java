package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.MapOutboxChannelRegistry;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxHealthIndicatorTest {

    @Test
    void upWhenBelowThreshold() {
        OutboxHealthIndicator indicator = indicator(new StubQueue(1, 100));
        Health health = indicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void pressureStatusWhenAboveThreshold() {
        OutboxHealthIndicator indicator = indicator(new StubQueue(95, 100));
        Health health = indicator.health();
        assertThat(health.getStatus().getCode()).isEqualTo("OUTBOX_PRESSURE");
        assertThat(health.getDetails()).containsKey("channels");
    }

    private static OutboxHealthIndicator indicator(OutboxDispatchQueue queue) {
        return new OutboxHealthIndicator(new MapOutboxChannelRegistry(
                Map.of("default", new DefaultOutboxChannel("default", null, queue, null, props(0.8)))));
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

    private static final class StubQueue implements OutboxDispatchQueue {
        private final int size;
        private final int capacity;

        StubQueue(int size, int capacity) {
            this.size = size;
            this.capacity = capacity;
        }

        @Override
        public boolean offer(long eventId) {
            return true;
        }

        @Override
        public Long poll(Duration timeout) {
            return null;
        }

        @Override
        public List<Long> drain(int max) {
            return List.of();
        }

        @Override
        public void acknowledge(Collection<Long> eventIds) {}

        @Override
        public int size() {
            return size;
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public double pressure() {
            return (double) size / capacity;
        }
    }
}
