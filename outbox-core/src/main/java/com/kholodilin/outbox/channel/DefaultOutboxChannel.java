package com.kholodilin.outbox.channel;

import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;

/**
 * Simple {@link OutboxChannel} holding wired collaborators.
 */
public final class DefaultOutboxChannel implements OutboxChannel {

    private final String name;
    private final OutboxStore store;
    private final OutboxDispatchQueue queue;
    private final OutboxSink sink;
    private final OutboxChannelProperties properties;

    public DefaultOutboxChannel(
            String name,
            OutboxStore store,
            OutboxDispatchQueue queue,
            OutboxSink sink,
            OutboxChannelProperties properties) {
        this.name = name;
        this.store = store;
        this.queue = queue;
        this.sink = sink;
        this.properties = properties;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public OutboxStore store() {
        return store;
    }

    @Override
    public OutboxDispatchQueue queue() {
        return queue;
    }

    @Override
    public OutboxSink sink() {
        return sink;
    }

    @Override
    public OutboxChannelProperties properties() {
        return properties;
    }
}
