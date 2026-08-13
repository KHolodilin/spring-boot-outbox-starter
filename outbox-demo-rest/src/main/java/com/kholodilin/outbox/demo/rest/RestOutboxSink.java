package com.kholodilin.outbox.demo.rest;

import java.util.List;

import com.kholodilin.outbox.autoconfigure.OutboxChannelSink;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

@OutboxChannelSink("webhooks")
public class RestOutboxSink implements OutboxSink {

    private final RestClient restClient;

    public RestOutboxSink(@Value("${demo.webhook.url}") String webhookUrl) {
        this.restClient = RestClient.builder().baseUrl(webhookUrl).build();
    }

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        try {
            for (OutboxRecord record : batch) {
                restClient
                        .post()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(record.payloadJson())
                        .retrieve()
                        .toBodilessEntity();
            }
            return new OutboxPublishResult.AllSucceeded();
        } catch (Exception ex) {
            return new OutboxPublishResult.AllFailed(ex);
        }
    }
}
