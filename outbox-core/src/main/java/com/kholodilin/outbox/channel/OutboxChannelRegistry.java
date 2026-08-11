package com.kholodilin.outbox.channel;

import java.util.Collection;
import java.util.Optional;

/**
 * Lookup of configured outbox channels.
 */
public interface OutboxChannelRegistry {

    OutboxChannel getRequired(String name);

    Optional<OutboxChannel> find(String name);

    Collection<OutboxChannel> all();
}
