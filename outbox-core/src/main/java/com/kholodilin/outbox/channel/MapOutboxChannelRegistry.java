package com.kholodilin.outbox.channel;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.kholodilin.outbox.exception.UnknownOutboxChannelException;

/**
 * Immutable registry backed by a name → channel map.
 */
public final class MapOutboxChannelRegistry implements OutboxChannelRegistry {

    private final Map<String, OutboxChannel> channels;

    public MapOutboxChannelRegistry(Map<String, OutboxChannel> channels) {
        this.channels = Map.copyOf(new LinkedHashMap<>(channels));
    }

    @Override
    public OutboxChannel getRequired(String name) {
        OutboxChannel channel = channels.get(name);
        if (channel == null) {
            throw new UnknownOutboxChannelException(name);
        }
        return channel;
    }

    @Override
    public Optional<OutboxChannel> find(String name) {
        return Optional.ofNullable(channels.get(name));
    }

    @Override
    public Collection<OutboxChannel> all() {
        return channels.values();
    }
}
