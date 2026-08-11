package com.kholodilin.outbox.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxMetricsTest {

    @Test
    void recordsCountersTimersAndGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(registry);
        AtomicInteger size = new AtomicInteger(3);
        metrics.registerQueueGauges("default", size, AtomicInteger::get, s -> s.get() / 10.0);

        metrics.incrementEnqueue("default", "ORDER_CREATED");
        metrics.incrementDequeue("default", "ORDER_CREATED");
        metrics.incrementPublish("default", "ORDER_CREATED", "success");
        metrics.recordPublishSeconds("default", "ORDER_CREATED", 1_000_000L);
        metrics.incrementRecovery("default", "ORDER_CREATED");

        assertThat(registry.find("outbox.enqueue").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.recovery").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.queue.size").gauge().value()).isEqualTo(3.0);
    }

    @Test
    void noopDoesNotThrow() {
        OutboxMetrics.noop().incrementEnqueue("c", "t");
        OutboxMetrics.noop().incrementPublish("c", "t", "failure");
    }
}
