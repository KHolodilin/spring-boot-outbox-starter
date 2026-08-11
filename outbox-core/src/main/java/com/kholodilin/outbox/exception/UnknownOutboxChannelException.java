package com.kholodilin.outbox.exception;

/**
 * Thrown when {@code OutboxService.channel(name)} refers to an unconfigured channel.
 */
public final class UnknownOutboxChannelException extends IllegalArgumentException {

    public UnknownOutboxChannelException(String channel) {
        super("Unknown outbox channel: " + channel);
    }
}
