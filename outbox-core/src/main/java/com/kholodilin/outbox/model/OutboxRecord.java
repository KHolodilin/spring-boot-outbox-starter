package com.kholodilin.outbox.model;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable view of an outbox row passed to {@link com.kholodilin.outbox.spi.OutboxSink}.
 */
public record OutboxRecord(
        String channel,
        long eventId,
        String eventType,
        String aggregateId,
        String partitionKey,
        String payloadJson,
        Map<String, String> headers,
        String traceParent,
        int retryCount,
        Instant createdAt) {}
