package com.kholodilin.outbox.exception;

/**
 * Thrown when {@code append()} is called outside an active Spring transaction.
 */
public final class MissingOutboxTransactionException extends IllegalStateException {

    public MissingOutboxTransactionException() {
        super("outbox append() requires an active database transaction");
    }
}
