package com.kholodilin.outbox;

/**
 * Entry point for appending outbox events inside a business transaction.
 */
public interface OutboxService {

    /** Selects channel; unknown name fails on append (or immediately). */
    OutboxAppend channel(String channel);

    /** Shorthand for {@code channel("default").eventType(eventType)}. */
    OutboxAppend eventType(String eventType);
}
