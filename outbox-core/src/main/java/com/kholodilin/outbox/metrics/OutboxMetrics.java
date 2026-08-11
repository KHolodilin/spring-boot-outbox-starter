package com.kholodilin.outbox.metrics;

import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Micrometer facade for outbox metrics (channel + eventType tags where applicable).
 *
 * <p>Prometheus names resolve to {@code outbox_*_total} / {@code outbox_publish_seconds} via
 * Micrometer naming.
 */
public final class OutboxMetrics {

    private final MeterRegistry registry;

    public OutboxMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public static OutboxMetrics noop() {
        return new OutboxMetrics(null);
    }

    public void incrementEnqueue(String channel, String eventType) {
        if (registry == null) {
            return;
        }
        Counter.builder("outbox.enqueue")
                .tag("channel", channel)
                .tag("eventType", nullToUnknown(eventType))
                .register(registry)
                .increment();
    }

    public void incrementDequeue(String channel, String eventType) {
        if (registry == null) {
            return;
        }
        Counter.builder("outbox.dequeue")
                .tag("channel", channel)
                .tag("eventType", nullToUnknown(eventType))
                .register(registry)
                .increment();
    }

    public void incrementPublish(String channel, String eventType, String result) {
        if (registry == null) {
            return;
        }
        Counter.builder("outbox.publish")
                .tag("channel", channel)
                .tag("eventType", nullToUnknown(eventType))
                .tag("result", result)
                .register(registry)
                .increment();
    }

    public void recordPublishSeconds(String channel, String eventType, long durationNanos) {
        if (registry == null) {
            return;
        }
        Timer.builder("outbox.publish")
                .tag("channel", channel)
                .tag("eventType", nullToUnknown(eventType))
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void incrementRecovery(String channel, String eventType) {
        if (registry == null) {
            return;
        }
        Counter.builder("outbox.recovery")
                .tag("channel", channel)
                .tag("eventType", nullToUnknown(eventType))
                .register(registry)
                .increment();
    }

    public <T> void registerQueueGauges(
            String channel, T queue, ToDoubleFunction<T> size, ToDoubleFunction<T> pressure) {
        if (registry == null) {
            return;
        }
        io.micrometer.core.instrument.Gauge.builder("outbox.queue.size", queue, size)
                .tag("channel", channel)
                .register(registry);
        io.micrometer.core.instrument.Gauge.builder("outbox.queue.pressure", queue, pressure)
                .tag("channel", channel)
                .register(registry);
    }

    private static String nullToUnknown(String eventType) {
        return eventType == null || eventType.isBlank() ? "unknown" : eventType;
    }
}
