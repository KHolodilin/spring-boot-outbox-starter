package com.kholodilin.outbox.autoconfigure.json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.spi.OutboxJson;

/**
 * Jackson 2 {@link OutboxJson} for Spring Boot 3.2+.
 */
public final class Jackson2OutboxJson implements OutboxJson {

    private final ObjectMapper mapper;

    public Jackson2OutboxJson(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to serialize outbox JSON", ex);
        }
    }

    @Override
    public Map<String, String> readStringMap(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            Map<String, String> map = new LinkedHashMap<>();
            node.fields()
                    .forEachRemaining(
                            entry -> map.put(entry.getKey(), entry.getValue().asText()));
            return Map.copyOf(map);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to read outbox headers JSON", ex);
        }
    }
}
