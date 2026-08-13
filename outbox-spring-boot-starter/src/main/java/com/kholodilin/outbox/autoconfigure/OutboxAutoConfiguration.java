package com.kholodilin.outbox.autoconfigure;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.channel.DefaultOutboxChannel;
import com.kholodilin.outbox.channel.MapOutboxChannelRegistry;
import com.kholodilin.outbox.channel.OutboxChannel;
import com.kholodilin.outbox.channel.OutboxChannelProperties;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import com.kholodilin.outbox.core.DefaultOutboxService;
import com.kholodilin.outbox.jdbc.JdbcOutboxStore;
import com.kholodilin.outbox.jdbc.OutboxSchemaManager;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.queue.memory.InMemoryOutboxDispatchQueue;
import com.kholodilin.outbox.queue.redis.RedisOutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxDispatchQueue;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.spi.OutboxStore;
import com.kholodilin.outbox.worker.PublisherWorker;
import com.kholodilin.outbox.worker.RecoveryWorker;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(
        afterName = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration"
        })
@ConditionalOnClass(DataSource.class)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "outbox", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    JsonMapper outboxJsonMapper() {
        return JsonMapper.builder().build();
    }

    @Bean
    @ConditionalOnMissingBean
    JdbcTemplate outboxJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxSchemaManager outboxSchemaManager(DataSource dataSource) {
        return new OutboxSchemaManager(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxMetrics outboxMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
        MeterRegistry registry = meterRegistry.getIfAvailable();
        return registry == null ? OutboxMetrics.noop() : new OutboxMetrics(registry);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    OutboxRuntime outboxRuntime(
            OutboxProperties properties,
            JdbcTemplate jdbcTemplate,
            OutboxSchemaManager schemaManager,
            JsonMapper jsonMapper,
            OutboxMetrics metrics,
            ApplicationContext applicationContext,
            ObjectProvider<StringRedisTemplate> redisTemplate) {
        Map<String, OutboxChannelProperties> channelProps = OutboxChannelConfigurer.resolveChannels(properties);
        Map<String, OutboxSink> sinks = resolveSinks(applicationContext);
        Map<String, OutboxChannel> channels = new LinkedHashMap<>();
        List<AutoCloseable> workers = new ArrayList<>();

        for (Map.Entry<String, OutboxChannelProperties> entry : channelProps.entrySet()) {
            String name = entry.getKey();
            OutboxChannelProperties props = entry.getValue();
            schemaManager.apply(props.tableName(), props.schemaMode());

            OutboxStore store = new JdbcOutboxStore(jdbcTemplate, props.tableName(), name, jsonMapper);
            OutboxDispatchQueue queue = createQueue(props, redisTemplate);
            metrics.registerQueueGauges(name, queue, OutboxDispatchQueue::size, OutboxDispatchQueue::pressure);

            OutboxSink sink = sinks.get(name);
            if (props.publisherEnabled() && sink == null) {
                throw new IllegalStateException(
                        "No OutboxSink bound for channel '" + name + "' (publisher.enabled=true). "
                                + "Add @OutboxChannelSink(\"" + name + "\") or a single OutboxSink bean for default.");
            }

            OutboxChannel channel = new DefaultOutboxChannel(name, store, queue, sink, props);
            channels.put(name, channel);

            if (props.publisherEnabled()) {
                PublisherWorker publisherWorker = new PublisherWorker(channel, properties.getInstanceId(), metrics);
                publisherWorker.start();
                workers.add(publisherWorker);
            }
            if (props.recoveryEnabled()) {
                RecoveryWorker recoveryWorker = new RecoveryWorker(channel, properties.getInstanceId(), metrics);
                recoveryWorker.start();
                workers.add(recoveryWorker);
            }
        }

        OutboxChannelRegistry registry = new MapOutboxChannelRegistry(channels);
        OutboxService service = new DefaultOutboxService(registry, jsonMapper, metrics);
        return new OutboxRuntime(registry, service, workers);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxChannelRegistry outboxChannelRegistry(OutboxRuntime runtime) {
        return runtime.registry();
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxService outboxService(OutboxRuntime runtime) {
        return runtime.service();
    }

    private static Map<String, OutboxSink> resolveSinks(ApplicationContext context) {
        Map<String, OutboxSink> sinks = new LinkedHashMap<>();
        Map<String, Object> annotated = context.getBeansWithAnnotation(OutboxChannelSink.class);
        for (Object bean : annotated.values()) {
            if (!(bean instanceof OutboxSink sink)) {
                throw new IllegalStateException("@OutboxChannelSink bean must implement OutboxSink: "
                        + bean.getClass().getName());
            }
            OutboxChannelSink annotation = org.springframework.core.annotation.AnnotationUtils.findAnnotation(
                    bean.getClass(), OutboxChannelSink.class);
            if (annotation == null) {
                throw new IllegalStateException(
                        "Missing @OutboxChannelSink on " + bean.getClass().getName());
            }
            if (sinks.put(annotation.value(), sink) != null) {
                throw new IllegalStateException("Duplicate OutboxSink for channel: " + annotation.value());
            }
        }
        if (sinks.isEmpty()) {
            Map<String, OutboxSink> plain = context.getBeansOfType(OutboxSink.class);
            if (plain.size() == 1) {
                sinks.put(
                        DefaultOutboxService.DEFAULT_CHANNEL,
                        plain.values().iterator().next());
            } else if (plain.size() > 1) {
                throw new IllegalStateException(
                        "Multiple OutboxSink beans found without @OutboxChannelSink; bind each explicitly.");
            }
        }
        return sinks;
    }

    private static OutboxDispatchQueue createQueue(
            OutboxChannelProperties props, ObjectProvider<StringRedisTemplate> redisTemplate) {
        OutboxChannelProperties.QueueType type = props.queueType();
        if (type == OutboxChannelProperties.QueueType.AUTO) {
            type = redisTemplate.getIfAvailable() == null
                    ? OutboxChannelProperties.QueueType.MEMORY
                    : OutboxChannelProperties.QueueType.REDIS;
        }
        if (type == OutboxChannelProperties.QueueType.REDIS) {
            StringRedisTemplate redis = redisTemplate.getIfAvailable();
            if (redis == null) {
                throw new IllegalStateException("Queue type redis requested for channel '" + props.name()
                        + "' but StringRedisTemplate is missing");
            }
            return new RedisOutboxDispatchQueue(redis, props.redisKeyPrefix(), props.queueCapacity());
        }
        return new InMemoryOutboxDispatchQueue(props.queueCapacity());
    }

    /** Holds registry/service and lifecycle of workers. */
    public static final class OutboxRuntime implements AutoCloseable {

        private final OutboxChannelRegistry registry;
        private final OutboxService service;
        private final List<AutoCloseable> workers;

        OutboxRuntime(OutboxChannelRegistry registry, OutboxService service, List<AutoCloseable> workers) {
            this.registry = registry;
            this.service = service;
            this.workers = List.copyOf(workers);
        }

        OutboxChannelRegistry registry() {
            return registry;
        }

        OutboxService service() {
            return service;
        }

        @Override
        public void close() {
            for (AutoCloseable worker : workers) {
                try {
                    worker.close();
                } catch (Exception ignored) {
                    // best-effort shutdown
                }
            }
        }
    }
}
