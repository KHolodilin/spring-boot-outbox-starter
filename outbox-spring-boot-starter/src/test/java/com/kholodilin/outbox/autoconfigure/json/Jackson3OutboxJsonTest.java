package com.kholodilin.outbox.autoconfigure.json;

import java.util.Map;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Jackson3OutboxJsonTest {

    private final Jackson3OutboxJson json =
            new Jackson3OutboxJson(JsonMapper.builder().build());

    @Test
    void serializesAndReadsStringMap() {
        assertThat(json.toJson(Map.of("ok", true))).isEqualTo("{\"ok\":true}");
        assertThat(json.readStringMap("{\"k\":\"v\"}")).containsEntry("k", "v");
    }

    @Test
    void rejectsInvalidHeadersJson() {
        assertThatThrownBy(() -> json.readStringMap("not-json")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toJsonWrapsMapperFailures() {
        assertThatThrownBy(() -> json.toJson(new ExplodingPayload()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to serialize outbox JSON");
    }

    static final class ExplodingPayload {
        public String getName() {
            throw new IllegalStateException("cannot serialize");
        }
    }
}
