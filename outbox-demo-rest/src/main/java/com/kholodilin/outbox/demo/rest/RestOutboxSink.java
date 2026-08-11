package com.kholodilin.outbox.demo.rest;

import java.util.List;

import com.kholodilin.outbox.autoconfigure.OutboxChannelSink;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.reactive.function.client.WebClient;

@OutboxChannelSink("webhooks")
public class RestOutboxSink implements OutboxSink {

    private final WebClient webClient;

    public RestOutboxSink(WebClient.Builder builder, @Value("${demo.webhook.url}") String webhookUrl) {
        this.webClient = builder.baseUrl(webhookUrl).build();
    }

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        try {
            for (OutboxRecord record : batch) {
                webClient
                        .post()
                        .uri("")
                        .bodyValue(record.payloadJson())
                        .retrieve()
                        .toBodilessEntity()
                        .block();
            }
            return new OutboxPublishResult.AllSucceeded();
        } catch (Exception ex) {
            return new OutboxPublishResult.AllFailed(ex);
        }
    }
}
