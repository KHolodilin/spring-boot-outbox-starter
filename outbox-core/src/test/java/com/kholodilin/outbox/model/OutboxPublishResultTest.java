package com.kholodilin.outbox.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxPublishResultTest {

    @Test
    void sealedVariants() {
        OutboxPublishResult ok = new OutboxPublishResult.AllSucceeded();
        OutboxPublishResult fail = new OutboxPublishResult.AllFailed(new IllegalStateException("x"));
        assertThat(ok).isInstanceOf(OutboxPublishResult.AllSucceeded.class);
        assertThat(((OutboxPublishResult.AllFailed) fail).cause()).hasMessage("x");
    }
}
