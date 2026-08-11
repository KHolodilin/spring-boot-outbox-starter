package com.kholodilin.outbox.channel;

import java.time.Duration;
import java.util.Map;

import com.kholodilin.outbox.exception.UnknownOutboxChannelException;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class MapOutboxChannelRegistryTest {

    @Test
    void getRequiredAndFind() {
        OutboxChannel channel = new DefaultOutboxChannel(
                "orders", mock(OutboxStore.class), mock(OutboxDispatchQueue.class), null, props("orders"));
        MapOutboxChannelRegistry registry = new MapOutboxChannelRegistry(Map.of("orders", channel));

        assertThat(registry.getRequired("orders")).isSameAs(channel);
        assertThat(registry.find("orders")).contains(channel);
        assertThat(registry.find("missing")).isEmpty();
        assertThat(registry.all()).containsExactly(channel);
        assertThatThrownBy(() -> registry.getRequired("missing"))
                .isInstanceOf(UnknownOutboxChannelException.class)
                .hasMessageContaining("missing");
    }

    private static OutboxChannelProperties props(String name) {
        return new OutboxChannelProperties(
                name,
                "outbox_events_" + name,
                OutboxChannelProperties.SchemaMode.VALIDATE,
                OutboxChannelProperties.QueueType.MEMORY,
                100,
                10,
                Duration.ofMillis(50),
                0.8,
                "outbox:",
                true,
                Duration.ofSeconds(30),
                5,
                true,
                Duration.ofSeconds(10),
                500);
    }
}
