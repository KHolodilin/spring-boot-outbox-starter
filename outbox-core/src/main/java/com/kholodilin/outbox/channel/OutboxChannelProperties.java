package com.kholodilin.outbox.channel;

import java.time.Duration;

/**
 * Resolved runtime settings for one outbox channel after defaults merge.
 * <p>
 * Bound from {@code outbox.defaults.*} and {@code outbox.channels.<name>.*}
 * (see {@code OutboxProperties}).
 */
public final class OutboxChannelProperties {

    /** Logical channel name (e.g. {@code default}, {@code orders}). */
    private final String name;

    /** PostgreSQL table for this channel (table-per-channel). */
    private final String tableName;

    /** Startup schema policy for {@link #tableName}. */
    private final SchemaMode schemaMode;

    /**
     * Wake-up queue kind. {@link QueueType#AUTO} is resolved to {@link QueueType#MEMORY}
     * or {@link QueueType#REDIS} before the queue bean is created.
     */
    private final QueueType queueType;

    /** Max event ids in the wake-up queue before {@code offer} rejects. */
    private final int queueCapacity;

    /** Max ids drained into one publisher batch after the first poll. */
    private final int batchSize;

    /** Wait for the first id in the publisher loop. */
    private final Duration batchWait;

    /** Fill ratio {@code [0..1]} used for health / pressure signalling. */
    private final double usageThreshold;

    /** Redis key prefix when {@link #queueType} is Redis (after AUTO resolution). */
    private final String redisKeyPrefix;

    /** When false, no publisher worker is started for this channel. */
    private final boolean publisherEnabled;

    /** Claim lease duration written to {@code locked_until}. */
    private final Duration leaseDuration;

    /** Failed publish attempts before status {@code DEAD}. */
    private final int maxRetries;

    /** When false, recovery does not run for this channel. */
    private final boolean recoveryEnabled;

    /** Delay between recovery ticks. */
    private final Duration recoveryInterval;

    /** Max recoverable ids per recovery tick. */
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

    /**
     * Startup handling of the channel table.
     * <ul>
     *   <li>{@link #CREATE} — apply DDL when the table is missing</li>
     *   <li>{@link #VALIDATE} — fail startup if the table is missing or incompatible</li>
     *   <li>{@link #NONE} — skip schema work (Flyway/Liquibase owned by the app)</li>
     * </ul>
     */
    public enum SchemaMode {
        CREATE,
        VALIDATE,
        NONE
    }

    /**
     * Wake-up queue implementation selected via {@code outbox.*.queue.type}.
     * <ul>
     *   <li>{@link #MEMORY} — in-process queue (single JVM)</li>
     *   <li>{@link #REDIS} — shared Redis wake-up; requires {@code StringRedisTemplate}</li>
     *   <li>{@link #AUTO} — resolve to {@link #REDIS} if {@code StringRedisTemplate} exists,
     *       otherwise {@link #MEMORY}</li>
     * </ul>
     */
    public enum QueueType {
        MEMORY,
        REDIS,
        AUTO
    }
}
