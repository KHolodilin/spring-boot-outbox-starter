package com.kholodilin.outbox.autoconfigure.json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.kholodilin.outbox.spi.OutboxJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson 3 {@link OutboxJson} for Spring Boot 4.x.
 */
public final class Jackson3OutboxJson implements OutboxJson {

    private final JsonMapper mapper;

    public Jackson3OutboxJson(JsonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Failed to serialize outbox JSON", ex);
        }
    }

    @Override
    public Map<String, String> readStringMap(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            Map<String, String> map = new LinkedHashMap<>();
            node.properties()
                    .forEach(entry -> map.put(entry.getKey(), entry.getValue().asString()));
            return Map.copyOf(map);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Failed to read outbox headers JSON", ex);
        }
    }
}
