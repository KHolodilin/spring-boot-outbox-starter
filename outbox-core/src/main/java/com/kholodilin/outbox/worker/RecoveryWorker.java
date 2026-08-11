package com.kholodilin.outbox.worker;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.kholodilin.outbox.channel.OutboxChannel;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-channel recovery: claim recoverable ACTIVE ids → clearLease → offer to the same queue.
 *
 * <p>Never calls {@link com.kholodilin.outbox.spi.OutboxSink}.
 */
public final class RecoveryWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RecoveryWorker.class);

    private final OutboxChannel channel;
    private final String instanceId;
    private final OutboxMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    public RecoveryWorker(OutboxChannel channel, String instanceId, OutboxMetrics metrics) {
        this.channel = channel;
        this.instanceId = instanceId;
        this.metrics = metrics == null ? OutboxMetrics.noop() : metrics;
    }

    public void start() {
        if (!channel.properties().recoveryEnabled()) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long intervalMs = Math.max(100L, channel.properties().recoveryInterval().toMillis());
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "outbox-recovery-" + channel.name());
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::safeRecover, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        running.set(false);
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /** Exposed for tests / manual ticks. */
    public int recover() {
        if (!channel.properties().recoveryEnabled()) {
            return 0;
        }
        Instant lockedUntil = Instant.now().plus(channel.properties().leaseDuration());
        List<Long> ids =
                channel.store().claimRecoverableIds(channel.properties().recoveryBatchSize(), instanceId, lockedUntil);
        if (ids.isEmpty()) {
            return 0;
        }

        channel.store().clearLease(ids);

        Map<Long, String> eventTypes = new ConcurrentHashMap<>();
        for (OutboxRecord record : channel.store().findByIds(ids)) {
            eventTypes.put(record.eventId(), record.eventType());
        }

        int enqueued = 0;
        for (Long id : ids) {
            if (channel.queue().offer(id)) {
                enqueued++;
                metrics.incrementRecovery(channel.name(), eventTypes.get(id));
            }
        }
        log.info("Recovery enqueued channel={} count={}", channel.name(), enqueued);
        return enqueued;
    }

    private void safeRecover() {
        if (!running.get()) {
            return;
        }
        try {
            recover();
        } catch (Exception ex) {
            log.error("Recovery tick failed channel={}", channel.name(), ex);
        }
    }
}
