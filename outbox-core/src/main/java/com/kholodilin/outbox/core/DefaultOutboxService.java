package com.kholodilin.outbox.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.kholodilin.outbox.OutboxAppend;
import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.channel.OutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import com.kholodilin.outbox.exception.MissingOutboxTransactionException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.model.OutboxInsert;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

/**
 * Default fluent {@link OutboxService} implementation.
 */
public final class DefaultOutboxService implements OutboxService {

    public static final String DEFAULT_CHANNEL = "default";

    private final OutboxChannelRegistry registry;
    private final JsonMapper objectMapper;
    private final OutboxMetrics metrics;

    public DefaultOutboxService(OutboxChannelRegistry registry, JsonMapper objectMapper, OutboxMetrics metrics) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.metrics = metrics == null ? OutboxMetrics.noop() : metrics;
    }

    @Override
    public OutboxAppend channel(String channel) {
        return new AppendBuilder(registry.getRequired(channel));
    }

    @Override
    public OutboxAppend eventType(String eventType) {
        return channel(DEFAULT_CHANNEL).eventType(eventType);
    }

    private final class AppendBuilder implements OutboxAppend {

        private final OutboxChannel channel;
        private String eventType;
        private String aggregateId;
        private String partitionKey;
        private String payloadJson;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private String traceParent;

        private AppendBuilder(OutboxChannel channel) {
            this.channel = channel;
        }

        @Override
        public OutboxAppend eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        @Override
        public OutboxAppend aggregateId(String aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        @Override
        public OutboxAppend partitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        @Override
        public OutboxAppend payload(String json) {
            this.payloadJson = json;
            return this;
        }

        @Override
        public OutboxAppend payload(Object value) {
            try {
                this.payloadJson = objectMapper.writeValueAsString(value);
            } catch (Exception ex) {
                throw new IllegalArgumentException("Failed to serialize outbox payload", ex);
            }
            return this;
        }

        @Override
        public OutboxAppend header(String name, String value) {
            this.headers.put(name, value);
            return this;
        }

        @Override
        public OutboxAppend headers(Map<String, String> headers) {
            if (headers != null) {
                this.headers.putAll(headers);
            }
            return this;
        }

        @Override
        public OutboxAppend traceParent(String traceParent) {
            this.traceParent = traceParent;
            return this;
        }

        @Override
        public long append() {
            requireActiveTransaction();
            requireField(eventType, "eventType");
            requireField(aggregateId, "aggregateId");
            requireField(partitionKey, "partitionKey");
            requireField(payloadJson, "payload");

            OutboxInsert insert = new OutboxInsert(
                    eventType, aggregateId, partitionKey, payloadJson, Map.copyOf(headers), traceParent);
            long eventId = channel.store().insert(insert);
            String channelName = channel.name();
            String type = eventType;
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    if (channel.queue().offer(eventId)) {
                        metrics.incrementEnqueue(channelName, type);
                    }
                }
            });
            return eventId;
        }

        private static void requireActiveTransaction() {
            if (!TransactionSynchronizationManager.isSynchronizationActive()
                    || !TransactionSynchronizationManager.isActualTransactionActive()) {
                throw new MissingOutboxTransactionException();
            }
        }

        private static void requireField(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalStateException("outbox append requires " + name);
            }
        }
    }
}
