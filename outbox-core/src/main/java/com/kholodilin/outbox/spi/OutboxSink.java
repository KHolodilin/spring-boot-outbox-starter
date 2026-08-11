package com.kholodilin.outbox.spi;

import java.util.List;

import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;

/**
 * Application-owned delivery adapter bound to one outbox channel.
 *
 * <p>Must not update outbox tables; must tolerate at-least-once delivery for the same {@code
 * eventId}.
 */
@FunctionalInterface
public interface OutboxSink {

    OutboxPublishResult publish(List<OutboxRecord> batch);
}
