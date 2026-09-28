package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.core.DefaultOutboxService;

final class OutboxChannelConfigurer {

    private OutboxChannelConfigurer() {}

    static Map<String, OutboxChannelProperties> resolveChannels(OutboxProperties properties) {
        Map<String, OutboxChannelProperties> resolved = new LinkedHashMap<>();
        if (properties.getChannels() == null || properties.getChannels().isEmpty()) {
            resolved.put(
                    DefaultOutboxService.DEFAULT_CHANNEL,
                    merge(DefaultOutboxService.DEFAULT_CHANNEL, properties.getDefaults(), null));
            return resolved;
        }
        for (Map.Entry<String, OutboxProperties.Channel> entry :
                properties.getChannels().entrySet()) {
            resolved.put(entry.getKey(), merge(entry.getKey(), properties.getDefaults(), entry.getValue()));
        }
        return resolved;
    }

    private static OutboxChannelProperties merge(
            String name, OutboxProperties.Defaults defaults, OutboxProperties.Channel channel) {
        OutboxProperties.Persistence persistence = channel != null && channel.getPersistence() != null
                ? channel.getPersistence()
                : new OutboxProperties.Persistence();
        OutboxProperties.Queue queue =
                channel != null && channel.getQueue() != null ? channel.getQueue() : new OutboxProperties.Queue();
        OutboxProperties.Publisher publisher = channel != null && channel.getPublisher() != null
                ? channel.getPublisher()
                : new OutboxProperties.Publisher();
        OutboxProperties.Recovery recovery = channel != null && channel.getRecovery() != null
                ? channel.getRecovery()
                : new OutboxProperties.Recovery();

        String tableName = firstNonBlank(
                persistence.getTableName(), defaults.getPersistence().getTableName(), defaultTableName(name));
        String schemaMode = firstNonBlank(
                persistence.getSchema() != null ? persistence.getSchema().getMode() : null,
                defaults.getPersistence().getSchema().getMode(),
                "validate");
        String queueType = firstNonBlank(queue.getType(), defaults.getQueue().getType(), "memory");
        int capacity = firstNonNull(queue.getCapacity(), defaults.getQueue().getCapacity(), 10000);
        int batchSize = firstNonNull(queue.getBatchSize(), defaults.getQueue().getBatchSize(), 250);
        Duration batchWait =
                firstNonNull(queue.getBatchWait(), defaults.getQueue().getBatchWait(), Duration.ofMillis(50));
        double usageThreshold =
                firstNonNull(queue.getUsageThreshold(), defaults.getQueue().getUsageThreshold(), 0.8d);
        String redisPrefix = firstNonBlank(
                queue.getRedis() != null ? queue.getRedis().getKeyPrefix() : null,
                defaults.getQueue().getRedis().getKeyPrefix(),
                "outbox:" + name + ":");
        boolean publisherEnabled =
                firstNonNull(publisher.getEnabled(), defaults.getPublisher().getEnabled(), true);
        Duration lease = firstNonNull(
                publisher.getLeaseDuration(), defaults.getPublisher().getLeaseDuration(), Duration.ofSeconds(30));
        int maxRetries =
                firstNonNull(publisher.getMaxRetries(), defaults.getPublisher().getMaxRetries(), 5);
        boolean recoveryEnabled =
                firstNonNull(recovery.getEnabled(), defaults.getRecovery().getEnabled(), true);
        Duration recoveryInterval =
                firstNonNull(recovery.getInterval(), defaults.getRecovery().getInterval(), Duration.ofSeconds(10));
        int recoveryBatch =
                firstNonNull(recovery.getBatchSize(), defaults.getRecovery().getBatchSize(), 500);

        return new OutboxChannelProperties(
                name,
                tableName,
                OutboxChannelProperties.SchemaMode.valueOf(schemaMode.trim().toUpperCase(Locale.ROOT)),
                OutboxChannelProperties.QueueType.valueOf(queueType.trim().toUpperCase(Locale.ROOT)),
                capacity,
                batchSize,
                batchWait,
                usageThreshold,
                redisPrefix,
                publisherEnabled,
                lease,
                maxRetries,
                recoveryEnabled,
                recoveryInterval,
                recoveryBatch);
    }

    private static String defaultTableName(String channel) {
        if (DefaultOutboxService.DEFAULT_CHANNEL.equals(channel)) {
            return "outbox_events";
        }
        return "outbox_events_" + channel.replace('-', '_');
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
