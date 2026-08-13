package com.kholodilin.outbox.channel;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxChannelPropertiesTest {

    @Test
    void exposesConfiguredValues() {
        OutboxChannelProperties props = new OutboxChannelProperties(
                "orders",
                "outbox_events_orders",
                OutboxChannelProperties.SchemaMode.VALIDATE,
                OutboxChannelProperties.QueueType.AUTO,
                1000,
                25,
                Duration.ofMillis(40),
                0.75,
                "outbox:orders:",
                true,
                Duration.ofSeconds(45),
                8,
                true,
                Duration.ofSeconds(15),
                250);

        assertThat(props.name()).isEqualTo("orders");
        assertThat(props.tableName()).isEqualTo("outbox_events_orders");
        assertThat(props.schemaMode()).isEqualTo(OutboxChannelProperties.SchemaMode.VALIDATE);
        assertThat(props.queueType()).isEqualTo(OutboxChannelProperties.QueueType.AUTO);
        assertThat(props.queueCapacity()).isEqualTo(1000);
        assertThat(props.batchSize()).isEqualTo(25);
        assertThat(props.batchWait()).isEqualTo(Duration.ofMillis(40));
        assertThat(props.usageThreshold()).isEqualTo(0.75);
        assertThat(props.redisKeyPrefix()).isEqualTo("outbox:orders:");
        assertThat(props.publisherEnabled()).isTrue();
        assertThat(props.leaseDuration()).isEqualTo(Duration.ofSeconds(45));
        assertThat(props.maxRetries()).isEqualTo(8);
        assertThat(props.recoveryEnabled()).isTrue();
        assertThat(props.recoveryInterval()).isEqualTo(Duration.ofSeconds(15));
        assertThat(props.recoveryBatchSize()).isEqualTo(250);
    }
}
