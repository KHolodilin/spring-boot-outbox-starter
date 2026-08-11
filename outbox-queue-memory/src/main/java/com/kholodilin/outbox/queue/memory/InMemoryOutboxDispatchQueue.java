package com.kholodilin.outbox.queue.memory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-process bounded wake-up queue with dedup and in-flight tracking.
 */
public final class InMemoryOutboxDispatchQueue implements OutboxDispatchQueue {

    private static final Logger log = LoggerFactory.getLogger(InMemoryOutboxDispatchQueue.class);

    private final int capacity;
    private final BlockingQueue<Long> queue;
    private final Set<Long> dedup = ConcurrentHashMap.newKeySet();
    private final Set<Long> inFlight = ConcurrentHashMap.newKeySet();

    public InMemoryOutboxDispatchQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public boolean offer(long eventId) {
        if (inFlight.contains(eventId)) {
            log.debug("In-flight enqueue ignored eventId={}", eventId);
            return false;
        }
        if (!dedup.add(eventId)) {
            log.debug("Duplicate enqueue ignored eventId={}", eventId);
            return false;
        }
        boolean offered = queue.offer(eventId);
        if (!offered) {
            dedup.remove(eventId);
            log.warn("Memory queue full, rejected eventId={}", eventId);
            return false;
        }
        return true;
    }

    @Override
    public Long poll(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        Long eventId = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (eventId != null) {
            dedup.remove(eventId);
            inFlight.add(eventId);
        }
        return eventId;
    }

    @Override
    public List<Long> drain(int max) {
        if (max <= 0) {
            return List.of();
        }
        List<Long> batch = new ArrayList<>(max);
        queue.drainTo(batch, max);
        for (Long eventId : batch) {
            dedup.remove(eventId);
            inFlight.add(eventId);
        }
        return batch;
    }

    @Override
    public void acknowledge(Collection<Long> eventIds) {
        if (eventIds == null) {
            return;
        }
        for (Long eventId : eventIds) {
            inFlight.remove(eventId);
        }
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public double pressure() {
        return capacity == 0 ? 0.0 : (double) queue.size() / capacity;
    }
}
