package com.kholodilin.outbox.channel;

import java.time.Duration;

/**
 * Resolved runtime settings for one outbox channel after defaults merge.
 */
public final class OutboxChannelProperties {

    private final String name;
    private final String tableName;
    private final SchemaMode schemaMode;
    private final QueueType queueType;
    private final int queueCapacity;
    private final int batchSize;
    private final Duration batchWait;
    private final double usageThreshold;
    private final String redisKeyPrefix;
    private final boolean publisherEnabled;
    private final Duration leaseDuration;
    private final int maxRetries;
    private final boolean recoveryEnabled;
    private final Duration recoveryInterval;
    private final int recoveryBatchSize;

    public OutboxChannelProperties(
            String name,
            String tableName,
            SchemaMode schemaMode,
            QueueType queueType,
            int queueCapacity,
            int batchSize,
            Duration batchWait,
            double usageThreshold,
            String redisKeyPrefix,
            boolean publisherEnabled,
            Duration leaseDuration,
            int maxRetries,
            boolean recoveryEnabled,
            Duration recoveryInterval,
            int recoveryBatchSize) {
        this.name = name;
        this.tableName = tableName;
        this.schemaMode = schemaMode;
        this.queueType = queueType;
        this.queueCapacity = queueCapacity;
        this.batchSize = batchSize;
        this.batchWait = batchWait;
        this.usageThreshold = usageThreshold;
        this.redisKeyPrefix = redisKeyPrefix;
        this.publisherEnabled = publisherEnabled;
        this.leaseDuration = leaseDuration;
        this.maxRetries = maxRetries;
        this.recoveryEnabled = recoveryEnabled;
        this.recoveryInterval = recoveryInterval;
        this.recoveryBatchSize = recoveryBatchSize;
    }

    public String name() {
        return name;
    }

    public String tableName() {
        return tableName;
    }

    public SchemaMode schemaMode() {
        return schemaMode;
    }

    public QueueType queueType() {
        return queueType;
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public int batchSize() {
        return batchSize;
    }

    public Duration batchWait() {
        return batchWait;
    }

    public double usageThreshold() {
        return usageThreshold;
    }

    public String redisKeyPrefix() {
        return redisKeyPrefix;
    }

    public boolean publisherEnabled() {
        return publisherEnabled;
    }

    public Duration leaseDuration() {
        return leaseDuration;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public boolean recoveryEnabled() {
        return recoveryEnabled;
    }

    public Duration recoveryInterval() {
        return recoveryInterval;
    }

    public int recoveryBatchSize() {
        return recoveryBatchSize;
    }

    public enum SchemaMode {
        CREATE,
        VALIDATE,
        NONE
    }

    public enum QueueType {
        MEMORY,
        REDIS,
        AUTO
    }
}
