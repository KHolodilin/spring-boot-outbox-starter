package com.kholodilin.outbox.channel;

import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;

/**
 * Named isolated outbox pipeline: table + queue + optional sink.
 */
public interface OutboxChannel {

    String name();

    OutboxStore store();

    OutboxDispatchQueue queue();

    /** Present when publisher is enabled and a sink was bound. */
    OutboxSink sink();

    OutboxChannelProperties properties();
}
