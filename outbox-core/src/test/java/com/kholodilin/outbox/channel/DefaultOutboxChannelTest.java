package com.kholodilin.outbox.channel;

import java.time.Duration;

import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DefaultOutboxChannelTest {

    @Test
    void exposesCollaborators() {
        OutboxStore store = mock(OutboxStore.class);
        OutboxDispatchQueue queue = mock(OutboxDispatchQueue.class);
        OutboxSink sink = batch -> null;
        OutboxChannelProperties props = new OutboxChannelProperties(
                "orders",
                "outbox_events_orders",
                OutboxChannelProperties.SchemaMode.CREATE,
                OutboxChannelProperties.QueueType.MEMORY,
                1,
                1,
                Duration.ofMillis(1),
                0.5,
                "p:",
                true,
                Duration.ofSeconds(1),
                1,
                true,
                Duration.ofSeconds(1),
                1);
        DefaultOutboxChannel channel = new DefaultOutboxChannel("orders", store, queue, sink, props);
        assertThat(channel.name()).isEqualTo("orders");
        assertThat(channel.store()).isSameAs(store);
        assertThat(channel.queue()).isSameAs(queue);
        assertThat(channel.sink()).isSameAs(sink);
        assertThat(channel.properties()).isSameAs(props);
        assertThat(props.schemaMode()).isEqualTo(OutboxChannelProperties.SchemaMode.CREATE);
        assertThat(props.queueType()).isEqualTo(OutboxChannelProperties.QueueType.MEMORY);
    }
}
