package com.kholodilin.outbox.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxStatusTest {

    @Test
    void codesAndActiveArchiveSplit() {
        assertThat(OutboxStatus.NEW.getCode()).isEqualTo(0);
        assertThat(OutboxStatus.PROCESSING.getCode()).isEqualTo(1);
        assertThat(OutboxStatus.FAILED.getCode()).isEqualTo(2);
        assertThat(OutboxStatus.DEAD.getCode()).isEqualTo(101);
        assertThat(OutboxStatus.SENT.getCode()).isEqualTo(110);
        assertThat(OutboxStatus.NEW.isActive()).isTrue();
        assertThat(OutboxStatus.FAILED.isActive()).isTrue();
        assertThat(OutboxStatus.SENT.isActive()).isFalse();
        assertThat(OutboxStatus.DEAD.isActive()).isFalse();
    }

    @Test
    void fromCodeRoundTrip() {
        assertThat(OutboxStatus.fromCode(2)).isEqualTo(OutboxStatus.FAILED);
        assertThatThrownBy(() -> OutboxStatus.fromCode(999)).isInstanceOf(IllegalArgumentException.class);
    }
}
