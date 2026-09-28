package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;
import java.util.Map;

import com.kholodilin.outbox.channel.OutboxChannelProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxChannelConfigurerTest {

    @Test
    void emptyChannelsYieldsDefault() {
        OutboxProperties properties = new OutboxProperties();
        Map<String, OutboxChannelProperties> channels = OutboxChannelConfigurer.resolveChannels(properties);
        assertThat(channels).containsOnlyKeys("default");
        assertThat(channels.get("default").tableName()).isEqualTo("outbox_events");
        assertThat(channels.get("default").schemaMode()).isEqualTo(OutboxChannelProperties.SchemaMode.VALIDATE);
        assertThat(channels.get("default").queueType()).isEqualTo(OutboxChannelProperties.QueueType.MEMORY);
    }

    @Test
    void mergesDefaultsIntoNamedChannels() {
        OutboxProperties properties = new OutboxProperties();
        properties.getDefaults().getPersistence().getSchema().setMode("create");
        properties.getDefaults().getPublisher().setMaxRetries(9);
        OutboxProperties.Channel orders = new OutboxProperties.Channel();
        orders.getPersistence().setTableName("outbox_events_orders");
        orders.getPublisher().setLeaseDuration(Duration.ofSeconds(60));
        properties.getChannels().put("orders", orders);

        Map<String, OutboxChannelProperties> channels = OutboxChannelConfigurer.resolveChannels(properties);
        assertThat(channels).containsOnlyKeys("orders");
        OutboxChannelProperties ordersProps = channels.get("orders");
        assertThat(ordersProps.tableName()).isEqualTo("outbox_events_orders");
        assertThat(ordersProps.schemaMode()).isEqualTo(OutboxChannelProperties.SchemaMode.CREATE);
        assertThat(ordersProps.maxRetries()).isEqualTo(9);
        assertThat(ordersProps.leaseDuration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(ordersProps.redisKeyPrefix()).isEqualTo("outbox:orders:");
    }
}
