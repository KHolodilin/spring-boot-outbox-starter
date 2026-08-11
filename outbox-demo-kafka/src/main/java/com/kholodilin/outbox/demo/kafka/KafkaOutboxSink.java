package com.kholodilin.outbox.demo.kafka;

import java.util.List;

import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaOutboxSink implements OutboxSink {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public KafkaOutboxSink(
            KafkaTemplate<String, String> kafkaTemplate, @Value("${demo.kafka.topic:payments.events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        try {
            for (OutboxRecord record : batch) {
                kafkaTemplate
                        .send(topic, record.partitionKey(), record.payloadJson())
                        .get();
            }
            return new OutboxPublishResult.AllSucceeded();
        } catch (Exception ex) {
            return new OutboxPublishResult.AllFailed(ex);
        }
    }
}
