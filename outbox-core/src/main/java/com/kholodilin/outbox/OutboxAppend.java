package com.kholodilin.outbox;

import java.util.Map;

/**
 * Fluent builder for a single outbox append within the current transaction.
 */
public interface OutboxAppend {

    OutboxAppend eventType(String eventType);

    OutboxAppend aggregateId(String aggregateId);

    OutboxAppend partitionKey(String partitionKey);

    OutboxAppend payload(String json);

    /** Jackson-serialize {@code value} to JSON text. */
    OutboxAppend payload(Object value);

    OutboxAppend header(String name, String value);

    OutboxAppend headers(Map<String, String> headers);

    OutboxAppend traceParent(String traceParent);

    /**
     * Inserts a NEW row in the current TX and registers afterCommit enqueue on the channel queue.
     *
     * @return generated event id
     */
    long append();
}
