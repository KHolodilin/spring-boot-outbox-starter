package com.kholodilin.outbox.spi;

import java.util.Map;

/**
 * JSON codec used by {@code payload(Object)} and JDBC header JSONB.
 *
 * <p>Implementations live in Spring Boot starters (Jackson 3 or Jackson 2). Core and JDBC must
 * not depend on a JSON library. Ready-made {@code payload(String)} is not passed through this SPI.
 */
public interface OutboxJson {

    /**
     * Serialize a value to a JSON object/array/scalar string.
     *
     * @throws IllegalArgumentException if {@code value} cannot be serialized
     */
    String toJson(Object value);

    /**
     * Parse a JSON object into a string map (header JSONB).
     *
     * @throws IllegalArgumentException if {@code json} is not a JSON object
     */
    Map<String, String> readStringMap(String json);
}
