package com.kholodilin.outbox.autoconfigure;

import java.util.LinkedHashMap;
import java.util.Map;

import com.kholodilin.outbox.channel.OutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Aggregate {@code outbox} health with per-channel details.
 */
public final class OutboxHealthIndicator implements HealthIndicator {

    private final OutboxChannelRegistry registry;

    public OutboxHealthIndicator(OutboxChannelRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Health health() {
        Map<String, Object> channels = new LinkedHashMap<>();
        boolean down = false;
        for (OutboxChannel channel : registry.all()) {
            double pressure = channel.queue().pressure();
            double threshold = channel.properties().usageThreshold();
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("pressure", pressure);
            details.put("queueSize", channel.queue().size());
            details.put("publisherEnabled", channel.properties().publisherEnabled());
            details.put("usageThreshold", threshold);
            if (pressure >= threshold) {
                details.put("status", "HIGH_PRESSURE");
                down = true;
            } else {
                details.put("status", "UP");
            }
            channels.put(channel.name(), details);
        }
        Health.Builder builder = down ? Health.status("OUTBOX_PRESSURE") : Health.up();
        return builder.withDetail("channels", channels).build();
    }
}
