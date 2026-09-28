package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Root configuration under {@code outbox.*}.
 * <p>
 * Empty {@link #channels} creates an implicit channel named {@code default}
 * (table {@code outbox_events}). Named channels each get their own table, queue,
 * publisher and recovery loop after merging with {@link #defaults}.
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    /**
     * Master switch. When {@code false}, the starter does not register outbox beans.
     * Property: {@code outbox.enabled}.
     */
    private boolean enabled = true;

    /**
     * Publisher / recovery instance id used for row leases ({@code locked_by}).
     * Prefer a unique value per pod (e.g. {@code ${HOSTNAME}}).
     * Property: {@code outbox.instance-id}.
     */
    private String instanceId = "local";

    /**
     * Defaults merged into every channel (and into the implicit {@code default} channel).
     * Property prefix: {@code outbox.defaults.*}.
     */
    private Defaults defaults = new Defaults();

    /**
     * Named channels. Empty map → single implicit channel {@code default}.
     * Property prefix: {@code outbox.channels.<name>.*}.
     */
    private Map<String, Channel> channels = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Defaults getDefaults() {
        return defaults;
    }

    public void setDefaults(Defaults defaults) {
        this.defaults = defaults;
    }

    public Map<String, Channel> getChannels() {
        return channels;
    }

    public void setChannels(Map<String, Channel> channels) {
        this.channels = channels;
    }

    /** Shared defaults applied before per-channel overrides. */
    public static class Defaults {
        private Persistence persistence = defaultPersistence();
        private Queue queue = defaultQueue();
        private Publisher publisher = defaultPublisher();
        private Recovery recovery = defaultRecovery();

        private static Persistence defaultPersistence() {
            Persistence persistence = new Persistence();
            persistence.getSchema().setMode("validate");
            return persistence;
        }

        private static Queue defaultQueue() {
            Queue queue = new Queue();
            queue.setType("memory");
            queue.setCapacity(10000);
            queue.setBatchSize(250);
            queue.setBatchWait(Duration.ofMillis(50));
            queue.setUsageThreshold(0.8d);
            return queue;
        }

        private static Publisher defaultPublisher() {
            Publisher publisher = new Publisher();
            publisher.setEnabled(true);
            publisher.setLeaseDuration(Duration.ofSeconds(30));
            publisher.setMaxRetries(5);
            return publisher;
        }

        private static Recovery defaultRecovery() {
            Recovery recovery = new Recovery();
            recovery.setEnabled(true);
            recovery.setInterval(Duration.ofSeconds(10));
            recovery.setBatchSize(500);
            return recovery;
        }

        public Persistence getPersistence() {
            return persistence;
        }

        public void setPersistence(Persistence persistence) {
            this.persistence = persistence;
        }

        public Queue getQueue() {
            return queue;
        }

        public void setQueue(Queue queue) {
            this.queue = queue;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        public void setPublisher(Publisher publisher) {
            this.publisher = publisher;
        }

        public Recovery getRecovery() {
            return recovery;
        }

        public void setRecovery(Recovery recovery) {
            this.recovery = recovery;
        }
    }

    /** Per-channel overrides; unset fields fall back to {@link Defaults}. */
    public static class Channel {
        private Persistence persistence = new Persistence();
        private Queue queue = new Queue();
        private Publisher publisher = new Publisher();
        private Recovery recovery = new Recovery();

        public Persistence getPersistence() {
            return persistence;
        }

        public void setPersistence(Persistence persistence) {
            this.persistence = persistence;
        }

        public Queue getQueue() {
            return queue;
        }

        public void setQueue(Queue queue) {
            this.queue = queue;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        public void setPublisher(Publisher publisher) {
            this.publisher = publisher;
        }

        public Recovery getRecovery() {
            return recovery;
        }

        public void setRecovery(Recovery recovery) {
            this.recovery = recovery;
        }
    }

    /** JDBC table and schema management for one channel. */
    public static class Persistence {

        /**
         * Outbox table name for this channel (table-per-channel).
         * Default channel uses {@code outbox_events} when unset.
         * Property: {@code outbox.channels.<name>.persistence.table-name}.
         */
        private String tableName;

        private Schema schema = new Schema();

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Schema getSchema() {
            return schema;
        }

        public void setSchema(Schema schema) {
            this.schema = schema;
        }
    }

    /** How the starter treats the channel table at startup. */
    public static class Schema {

        /**
         * Schema mode: {@code create} (run DDL), {@code validate} (fail if missing/incompatible),
         * or {@code none} (you own migrations). When null on a channel, falls back to defaults
         * (then {@code validate}).
         * Property: {@code outbox.*.persistence.schema.mode}.
         */
        private String mode;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }
    }

    /** Wake-up dispatch queue between after-commit and the publisher worker. */
    public static class Queue {

        /**
         * Queue implementation: {@code memory}, {@code redis}, or {@code auto}.
         * <ul>
         *   <li>{@code memory} — in-process only (one JVM)</li>
         *   <li>{@code redis} — shared wake-up; requires {@code StringRedisTemplate}</li>
         *   <li>{@code auto} — {@code redis} if {@code StringRedisTemplate} is present, else {@code memory}</li>
         * </ul>
         * When null on a channel, falls back to defaults (then {@code memory}).
         * Property: {@code outbox.*.queue.type}.
         */
        private String type;

        /**
         * Max event ids held in the wake-up queue. When full, {@code offer} returns false
         * and recovery must pick the row up later.
         * Property: {@code outbox.*.queue.capacity}.
         */
        private Integer capacity;

        /**
         * Max ids drained into one publisher batch after the first poll.
         * Property: {@code outbox.*.queue.batch-size}.
         */
        private Integer batchSize;

        /**
         * How long the publisher waits for the first id before looping again.
         * Property: {@code outbox.*.queue.batch-wait}.
         */
        private Duration batchWait;

        /**
         * Queue fill ratio in {@code [0..1]} above which health / backpressure may signal pressure.
         * Property: {@code outbox.*.queue.usage-threshold}.
         */
        private Double usageThreshold;

        private Redis redis = new Redis();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Integer getCapacity() {
            return capacity;
        }

        public void setCapacity(Integer capacity) {
            this.capacity = capacity;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getBatchWait() {
            return batchWait;
        }

        public void setBatchWait(Duration batchWait) {
            this.batchWait = batchWait;
        }

        public Double getUsageThreshold() {
            return usageThreshold;
        }

        public void setUsageThreshold(Double usageThreshold) {
            this.usageThreshold = usageThreshold;
        }

        public Redis getRedis() {
            return redis;
        }

        public void setRedis(Redis redis) {
            this.redis = redis;
        }
    }

    /** Redis-specific wake-up queue settings (used when queue type is {@code redis} or {@code auto}→redis). */
    public static class Redis {

        /**
         * Key prefix for Redis list / dedup structures.
         * When null, resolved to {@code outbox:<channel>:}.
         * Property: {@code outbox.*.queue.redis.key-prefix}.
         */
        private String keyPrefix;

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }
    }

    /** Publisher worker that drains the queue and calls {@code OutboxSink}. */
    public static class Publisher {

        /**
         * When {@code false}, append still works but no publisher thread is started (write-only).
         * Property: {@code outbox.*.publisher.enabled}.
         */
        private Boolean enabled;

        /**
         * How long a claimed row stays leased to this instance ({@code locked_until}).
         * Property: {@code outbox.*.publisher.lease-duration}.
         */
        private Duration leaseDuration;

        /**
         * After this many failed publish attempts the row becomes {@code DEAD}.
         * Property: {@code outbox.*.publisher.max-retries}.
         */
        private Integer maxRetries;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getLeaseDuration() {
            return leaseDuration;
        }

        public void setLeaseDuration(Duration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }

        public Integer getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
        }
    }

    /** Periodic re-enqueue of unpublished ACTIVE rows into the same channel queue. */
    public static class Recovery {

        /**
         * When {@code false}, the recovery loop does not run for the channel.
         * Property: {@code outbox.*.recovery.enabled}.
         */
        private Boolean enabled;

        /**
         * Delay between recovery ticks.
         * Property: {@code outbox.*.recovery.interval}.
         */
        private Duration interval;

        /**
         * Max recoverable ids claimed per tick.
         * Property: {@code outbox.*.recovery.batch-size}.
         */
        private Integer batchSize;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration interval) {
            this.interval = interval;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }
    }
}
