package com.kholodilin.outbox.demo.rest;

import java.time.Duration;
import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.kholodilin.outbox.model.OutboxStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class OutboxDemoRestIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static WireMockServer wireMock;

    @BeforeAll
    static void startWireMock() {
        wireMock = new WireMockServer(0);
        wireMock.start();
        wireMock.stubFor(
                post(urlEqualTo("/hooks/payments")).willReturn(aResponse().withStatus(500)));
    }

    @AfterAll
    static void stopWireMock() {
        if (wireMock != null) {
            wireMock.stop();
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.sql.init.mode", () -> "always");
        registry.add("outbox.defaults.persistence.schema.mode", () -> "create");
        registry.add("outbox.defaults.recovery.interval", () -> "2s");
        registry.add("outbox.defaults.publisher.max-retries", () -> "2");
        registry.add("demo.webhook.url", () -> "http://localhost:" + wireMock.port() + "/hooks/payments");
    }

    @LocalServerPort
    int port;

    RestClient http;

    @Autowired
    private PaymentsStubSink paymentsStubSink;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        http = RestClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void paymentsSucceedWhileWebhooksRetryIndependently() {
        @SuppressWarnings("unchecked")
        Map<String, Object> body = http.post()
                .uri("/payments")
                .body(Map.of("customerId", "c-9"))
                .retrieve()
                .body(Map.class);
        assertThat(body).containsKeys("paymentId", "paymentEventId", "webhookEventId");

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(paymentsStubSink.published()).isNotEmpty());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Integer dead = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM outbox_events_webhooks WHERE status = ?",
                    Integer.class,
                    OutboxStatus.DEAD.getCode());
            assertThat(dead).isGreaterThanOrEqualTo(1);
            wireMock.verify(postRequestedFor(urlEqualTo("/hooks/payments")));
        });

        Integer sentPayments = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events_payments WHERE status = ?",
                Integer.class,
                OutboxStatus.SENT.getCode());
        assertThat(sentPayments).isGreaterThanOrEqualTo(1);
    }
}
