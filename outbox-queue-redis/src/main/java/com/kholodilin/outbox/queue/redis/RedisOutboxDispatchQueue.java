package com.kholodilin.outbox.queue.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Shared Redis wake-up queue with fail-open semantics and local in-flight tracking.
 *
 * <p>On Redis errors, {@link #offer} / {@link #poll} / {@link #drain} return soft failures so
 * recovery can continue from PostgreSQL.
 */
public final class RedisOutboxDispatchQueue implements OutboxDispatchQueue {

    private static final Logger log = LoggerFactory.getLogger(RedisOutboxDispatchQueue.class);

    private final StringRedisTemplate redis;
    private final String listKey;
    private final String dedupKey;
    private final int capacity;
    private final Set<Long> inFlight = ConcurrentHashMap.newKeySet();

    public RedisOutboxDispatchQueue(StringRedisTemplate redis, String keyPrefix, int capacity) {
        this.redis = Objects.requireNonNull(redis, "redis");
        String prefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        if (!prefix.endsWith(":")) {
            prefix = prefix + ":";
        }
        this.listKey = prefix + "queue";
        this.dedupKey = prefix + "dedup";
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
    }

    @Override
    public boolean offer(long eventId) {
        if (inFlight.contains(eventId)) {
            return false;
        }
        try {
            Long added = redis.opsForSet().add(dedupKey, Long.toString(eventId));
            if (added == null || added == 0L) {
                return false;
            }
            Long size = redis.opsForList().size(listKey);
            if (size != null && size >= capacity) {
                redis.opsForSet().remove(dedupKey, Long.toString(eventId));
                return false;
            }
            redis.opsForList().rightPush(listKey, Long.toString(eventId));
            return true;
        } catch (RuntimeException ex) {
            log.warn("Redis offer fail-open eventId={}: {}", eventId, ex.toString());
            return false;
        }
    }

    @Override
    public Long poll(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Long id = tryLeftPop();
            if (id != null) {
                return id;
            }
            Thread.sleep(Math.min(50L, Math.max(1L, timeout.toMillis())));
        }
        return tryLeftPop();
    }

    @Override
    public List<Long> drain(int max) {
        if (max <= 0) {
            return List.of();
        }
        List<Long> batch = new ArrayList<>(max);
        try {
            for (int i = 0; i < max; i++) {
                Long id = tryLeftPop();
                if (id == null) {
                    break;
                }
                batch.add(id);
            }
        } catch (RuntimeException ex) {
            log.warn("Redis drain fail-open: {}", ex.toString());
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
        try {
            Long size = redis.opsForList().size(listKey);
            return size == null ? 0 : size.intValue();
        } catch (RuntimeException ex) {
            log.warn("Redis size fail-open: {}", ex.toString());
            return 0;
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public double pressure() {
        return capacity == 0 ? 0.0 : (double) size() / capacity;
    }

    private Long tryLeftPop() {
        try {
            String value = redis.opsForList().leftPop(listKey);
            if (value == null) {
                return null;
            }
            long eventId = Long.parseLong(value);
            redis.opsForSet().remove(dedupKey, value);
            inFlight.add(eventId);
            return eventId;
        } catch (RuntimeException ex) {
            log.warn("Redis poll fail-open: {}", ex.toString());
            return null;
        }
    }
}
