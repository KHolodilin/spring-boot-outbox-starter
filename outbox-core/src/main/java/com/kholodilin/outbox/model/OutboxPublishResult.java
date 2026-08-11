package com.kholodilin.outbox.model;

/**
 * Result of publishing a batch via {@link com.kholodilin.outbox.spi.OutboxSink}.
 *
 * <p>v1 is all-or-nothing; partial success is out of scope.
 */
public sealed interface OutboxPublishResult {

    /** Entire batch was delivered successfully. */
    record AllSucceeded() implements OutboxPublishResult {}

    /** Entire batch failed; {@code cause} may be {@code null}. */
    record AllFailed(Throwable cause) implements OutboxPublishResult {}
}
