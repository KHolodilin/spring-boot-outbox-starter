package com.kholodilin.outbox.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcOutboxStoreValidateTest {

    @Test
    void rejectsInvalidTableName() {
        assertThatThrownBy(() -> JdbcOutboxStore.validateTableName("outbox;drop"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdbcOutboxStore.validateTableName(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
