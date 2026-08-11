package com.kholodilin.outbox.worker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.kholodilin.outbox.channel.OutboxChannel;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-channel background worker: poll → claim → sink.publish → mark status → ack.
 */
public final class PublisherWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PublisherWorker.class);

    private final OutboxChannel channel;
    private final String instanceId;
    private final OutboxMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public PublisherWorker(OutboxChannel channel, String instanceId, OutboxMetrics metrics) {
        this.channel = channel;
        this.instanceId = instanceId;
        this.metrics = metrics == null ? OutboxMetrics.noop() : metrics;
    }

    public void start() {
        if (!channel.properties().publisherEnabled()) {
            return;
        }
        if (channel.sink() == null) {
            throw new IllegalStateException("Publisher enabled but no OutboxSink bound for channel: " + channel.name());
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "outbox-publisher-" + channel.name());
            thread.setDaemon(true);
            return thread;
        });
        executor.submit(this::loop);
    }

    @Override
    public void close() {
        running.set(false);
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    void processOnce() throws InterruptedException {
        tick();
    }

    private void loop() {
        while (running.get()) {
            try {
                tick();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception ex) {
                log.error("Publisher loop error channel={}", channel.name(), ex);
            }
        }
    }

    private void tick() throws InterruptedException {
        long batchWaitMs = channel.properties().batchWait().toMillis();
        int batchSize = channel.properties().batchSize();
        Long firstId = channel.queue().poll(java.time.Duration.ofMillis(batchWaitMs));
        if (firstId == null) {
            return;
        }
        List<Long> ids = new ArrayList<>();
        ids.add(firstId);
        ids.addAll(channel.queue().drain(Math.max(0, batchSize - 1)));

        try {
            Instant lockedUntil = Instant.now().plus(channel.properties().leaseDuration());
            List<OutboxRecord> claimed = channel.store().claimByIds(ids, instanceId, lockedUntil);
            if (claimed.isEmpty()) {
                for (Long id : channel.store().findReenqueueableIds(ids)) {
                    channel.queue().offer(id);
                }
                return;
            }

            for (OutboxRecord record : claimed) {
                metrics.incrementDequeue(channel.name(), record.eventType());
            }

            long start = System.nanoTime();
            OutboxPublishResult result;
            try {
                result = channel.sink().publish(claimed);
            } catch (Exception ex) {
                result = new OutboxPublishResult.AllFailed(ex);
            }
            long durationNs = System.nanoTime() - start;

            if (result instanceof OutboxPublishResult.AllSucceeded) {
                channel.store()
                        .markSent(claimed.stream().map(OutboxRecord::eventId).toList(), Instant.now());
                for (OutboxRecord record : claimed) {
                    metrics.incrementPublish(channel.name(), record.eventType(), "success");
                    metrics.recordPublishSeconds(channel.name(), record.eventType(), durationNs / claimed.size());
                }
            } else {
                handleFailures(claimed, durationNs);
            }
        } finally {
            channel.queue().acknowledge(ids);
        }
    }

    private void handleFailures(List<OutboxRecord> claimed, long durationNs) {
        int maxRetries = channel.properties().maxRetries();
        for (OutboxRecord record : claimed) {
            int nextRetry = record.retryCount() + 1;
            OutboxStatus status = nextRetry >= maxRetries ? OutboxStatus.DEAD : OutboxStatus.FAILED;
            channel.store().markFailed(record.eventId(), nextRetry, status);
            metrics.incrementPublish(channel.name(), record.eventType(), "failure");
            metrics.recordPublishSeconds(channel.name(), record.eventType(), durationNs / claimed.size());
            log.info(
                    "Outbox event marked {} channel={} eventId={} eventType={} retryCount={}",
                    status,
                    channel.name(),
                    record.eventId(),
                    record.eventType(),
                    nextRetry);
        }
    }
}
