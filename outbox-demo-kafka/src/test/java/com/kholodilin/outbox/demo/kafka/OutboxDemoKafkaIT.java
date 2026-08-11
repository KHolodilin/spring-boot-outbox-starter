package com.kholodilin.outbox.demo.kafka;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class OutboxDemoKafkaIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.sql.init.mode", () -> "always");
        registry.add("outbox.defaults.persistence.schema.mode", () -> "create");
        registry.add("outbox.defaults.recovery.interval", () -> "2s");
        registry.add("demo.kafka.topic", () -> "payments.events");
    }

    @LocalServerPort
    int port;

    RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void appendPublishesToKafka() {
        @SuppressWarnings("unchecked")
        Map<String, Object> body = http.post()
                .uri("/payments")
                .body(Map.of("customerId", "c-1"))
                .retrieve()
                .body(Map.class);
        assertThat(body).containsKeys("paymentId", "eventId");

        var consumerProps = KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "it", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (var consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            consumer.subscribe(java.util.List.of("payments.events"));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecord<String, String> record =
                        KafkaTestUtils.getSingleRecord(consumer, "payments.events", Duration.ofSeconds(5));
                assertThat(record.key()).isEqualTo("c-1");
                assertThat(record.value()).contains("paymentId");
            });
        }
    }
}
