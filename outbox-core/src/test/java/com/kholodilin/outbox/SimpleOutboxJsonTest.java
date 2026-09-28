package com.kholodilin.outbox;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimpleOutboxJsonTest {

    private final SimpleOutboxJson json = new SimpleOutboxJson();

    @Test
    void serializesMapAndPrimitives() {
        assertThat(json.toJson(Map.of("ok", true))).isEqualTo("{\"ok\":true}");
        assertThat(json.toJson("hi")).isEqualTo("\"hi\"");
        assertThat(json.toJson(null)).isEqualTo("null");
    }

    @Test
    void roundTripsStringMap() {
        String encoded = json.toJson(Map.of("k", "v"));
        assertThat(json.readStringMap(encoded)).containsEntry("k", "v");
        assertThat(json.readStringMap("{}")).isEmpty();
    }

    @Test
    void rejectsUnsupportedTypeAndBadJson() {
        assertThatThrownBy(() -> json.toJson(new Object())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> json.readStringMap("[]")).isInstanceOf(IllegalArgumentException.class);
    }
}
