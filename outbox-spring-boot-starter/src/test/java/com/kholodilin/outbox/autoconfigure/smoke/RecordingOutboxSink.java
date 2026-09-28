package com.kholodilin.outbox.autoconfigure.smoke;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;

final class RecordingOutboxSink implements OutboxSink {

    private final List<OutboxRecord> published = new CopyOnWriteArrayList<>();

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        published.addAll(batch);
        return new OutboxPublishResult.AllSucceeded();
    }

    List<OutboxRecord> published() {
        return List.copyOf(published);
    }
}
