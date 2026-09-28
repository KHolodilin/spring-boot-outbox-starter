package com.kholodilin.outbox.autoconfigure.json;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Jackson2OutboxJsonTest {

    private final Jackson2OutboxJson json = new Jackson2OutboxJson(new ObjectMapper());

    @Test
    void serializesAndReadsStringMap() {
        assertThat(json.toJson(Map.of("ok", true))).isEqualTo("{\"ok\":true}");
        assertThat(json.readStringMap("{\"k\":\"v\"}")).containsEntry("k", "v");
    }

    @Test
    void rejectsInvalidHeadersJson() {
        assertThatThrownBy(() -> json.readStringMap("not-json")).isInstanceOf(IllegalArgumentException.class);
    }
}
