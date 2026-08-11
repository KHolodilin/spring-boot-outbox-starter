package com.kholodilin.outbox.autoconfigure;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import com.kholodilin.outbox.OutboxService;
import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class OutboxAutoConfigurationIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        JdbcTemplateAutoConfiguration.class,
                        OutboxAutoConfiguration.class))
                .withPropertyValues(
                        "spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "spring.datasource.username=" + POSTGRES.getUsername(),
                        "spring.datasource.password=" + POSTGRES.getPassword(),
                        "spring.datasource.driver-class-name=org.postgresql.Driver",
                        "outbox.defaults.persistence.schema.mode=create",
                        "outbox.defaults.recovery.enabled=false",
                        "outbox.instance-id=test-pod");
    }

    @Test
    void defaultChannelWiresWithSingleSink() {
        runner().withUserConfiguration(SingleSinkConfig.class).run(context -> {
            assertThat(context).hasSingleBean(OutboxService.class);
            OutboxChannelRegistry registry = context.getBean(OutboxChannelRegistry.class);
            assertThat(registry.getRequired("default")).isNotNull();
            assertThat(registry.getRequired("default").properties().tableName()).isEqualTo("outbox_events");
        });
    }

    @Test
    void missingSinkFailsFast() {
        runner().run(context -> assertThat(context).hasFailed().getFailure().hasMessageContaining("default"));
    }

    @Test
    void multiChannelIsolation() {
        runner().withPropertyValues(
                        "outbox.channels.payments.persistence.table-name=outbox_events_payments",
                        "outbox.channels.webhooks.persistence.table-name=outbox_events_webhooks")
                .withUserConfiguration(MultiSinkConfig.class, TxConfig.class)
                .run(context -> {
                    OutboxService outbox = context.getBean(OutboxService.class);
                    TransactionTemplate tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
                    tx.executeWithoutResult(status -> {
                        outbox.channel("payments")
                                .eventType("PAYMENT_CREATED")
                                .aggregateId("1")
                                .partitionKey("c1")
                                .payload(Map.of("id", 1))
                                .append();
                        outbox.channel("webhooks")
                                .eventType("PAYMENT_WEBHOOK")
                                .aggregateId("1")
                                .partitionKey("c1")
                                .payload(Map.of("id", 1))
                                .append();
                    });
                    OutboxChannelRegistry registry = context.getBean(OutboxChannelRegistry.class);
                    assertThat(registry.all()).hasSize(2);
                    assertThat(registry.getRequired("payments").properties().tableName())
                            .isEqualTo("outbox_events_payments");
                });
    }

    @Test
    void unknownChannelFails() {
        runner().withUserConfiguration(SingleSinkConfig.class).run(context -> {
            OutboxService outbox = context.getBean(OutboxService.class);
            assertThatThrownBy(() -> outbox.channel("missing").eventType("X"))
                    .isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Configuration
    static class SingleSinkConfig {
        @Bean
        OutboxSink outboxSink() {
            return batch -> new OutboxPublishResult.AllSucceeded();
        }
    }

    @Configuration
    static class MultiSinkConfig {
        @Bean
        PaymentsSink paymentsSink() {
            return new PaymentsSink();
        }

        @Bean
        WebhooksSink webhooksSink() {
            return new WebhooksSink();
        }
    }

    @OutboxChannelSink("payments")
    static class PaymentsSink implements OutboxSink {
        @Override
        public OutboxPublishResult publish(List<OutboxRecord> batch) {
            return new OutboxPublishResult.AllSucceeded();
        }
    }

    @OutboxChannelSink("webhooks")
    static class WebhooksSink implements OutboxSink {
        @Override
        public OutboxPublishResult publish(List<OutboxRecord> batch) {
            return new OutboxPublishResult.AllSucceeded();
        }
    }

    @Configuration
    @EnableTransactionManagement
    static class TxConfig {
        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    private static OutboxPublishResult ok(List<OutboxRecord> batch) {
        return new OutboxPublishResult.AllSucceeded();
    }
}
