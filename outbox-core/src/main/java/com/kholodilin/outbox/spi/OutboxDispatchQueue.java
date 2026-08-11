package com.kholodilin.outbox.spi;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Best-effort wake-up queue of {@code eventId} values for one channel.
 *
 * <p>PostgreSQL remains the source of truth; {@code offer=false} is acceptable.
 */
public interface OutboxDispatchQueue {

    boolean offer(long eventId);

    Long poll(Duration timeout) throws InterruptedException;

    List<Long> drain(int max);

    void acknowledge(Collection<Long> eventIds);

    int size();

    int capacity();

    double pressure();
}
