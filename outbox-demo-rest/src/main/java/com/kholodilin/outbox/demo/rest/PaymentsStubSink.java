package com.kholodilin.outbox.demo.rest;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.kholodilin.outbox.autoconfigure.OutboxChannelSink;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;

@OutboxChannelSink("payments")
public class PaymentsStubSink implements OutboxSink {

    private final CopyOnWriteArrayList<OutboxRecord> published = new CopyOnWriteArrayList<>();

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        published.addAll(batch);
        return new OutboxPublishResult.AllSucceeded();
    }

    public List<OutboxRecord> published() {
        return List.copyOf(published);
    }
}
