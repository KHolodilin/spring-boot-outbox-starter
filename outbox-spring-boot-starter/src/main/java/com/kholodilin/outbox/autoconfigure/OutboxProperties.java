package com.kholodilin.outbox.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Root configuration under {@code outbox.*}.
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private boolean enabled = true;
    private String instanceId = "local";
    private Defaults defaults = new Defaults();
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

    public static class Persistence {
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

    public static class Schema {
        /** When null on a channel, falls back to defaults (then {@code validate}). */
        private String mode;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }
    }

    public static class Queue {
        /** When null on a channel, falls back to defaults (then {@code memory}). */
        private String type;

        private Integer capacity;
        private Integer batchSize;
        private Duration batchWait;
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

    public static class Redis {
        /** When null, resolved to {@code outbox:<channel>:}. */
        private String keyPrefix;

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }
    }

    public static class Publisher {
        private Boolean enabled;
        private Duration leaseDuration;
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

    public static class Recovery {
        private Boolean enabled;
        private Duration interval;
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
