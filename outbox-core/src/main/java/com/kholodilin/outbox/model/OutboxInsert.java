package com.kholodilin.outbox.model;

import java.util.Map;

/**
 * Data required to insert a new outbox row in the current transaction.
 */
public record OutboxInsert(
        String eventType,
        String aggregateId,
        String partitionKey,
        String payloadJson,
        Map<String, String> headers,
        String traceParent) {}
