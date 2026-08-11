package com.kholodilin.outbox.demo.rest;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.kholodilin.outbox.OutboxService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    private final OutboxService outboxService;
    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong ids = new AtomicLong();

    public PaymentController(OutboxService outboxService, JdbcTemplate jdbcTemplate) {
        this.outboxService = outboxService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("/payments")
    @Transactional
    public Map<String, Object> create(@RequestBody Map<String, Object> request) {
        long paymentId = ids.incrementAndGet();
        String customerId = String.valueOf(request.getOrDefault("customerId", "unknown"));
        jdbcTemplate.update(
                "INSERT INTO payments(id, customer_id) VALUES (?, ?) ON CONFLICT DO NOTHING", paymentId, customerId);

        long paymentEvent = outboxService
                .channel("payments")
                .eventType("PAYMENT_CREATED")
                .aggregateId(String.valueOf(paymentId))
                .partitionKey(customerId)
                .payload(Map.of("paymentId", paymentId))
                .append();

        long webhookEvent = outboxService
                .channel("webhooks")
                .eventType("PAYMENT_WEBHOOK")
                .aggregateId(String.valueOf(paymentId))
                .partitionKey(customerId)
                .payload(Map.of("paymentId", paymentId))
                .append();

        return Map.of("paymentId", paymentId, "paymentEventId", paymentEvent, "webhookEventId", webhookEvent);
    }
}
