package com.kholodilin.outbox.autoconfigure;

import com.kholodilin.outbox.channel.OutboxChannelRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = OutboxAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(OutboxChannelRegistry.class)
public class OutboxHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "outboxHealthIndicator")
    HealthIndicator outboxHealthIndicator(OutboxChannelRegistry registry) {
        return new OutboxHealthIndicator(registry);
    }
}
