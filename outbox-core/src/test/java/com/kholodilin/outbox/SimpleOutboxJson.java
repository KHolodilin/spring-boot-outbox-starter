package com.kholodilin.outbox;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.kholodilin.outbox.spi.OutboxJson;

/**
 * Test-only {@link OutboxJson} for maps and primitives, without Jackson.
 */
public final class SimpleOutboxJson implements OutboxJson {

    @Override
    public String toJson(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value);
        return sb.toString();
    }

    @Override
    public Map<String, String> readStringMap(String json) {
        JsonReader reader = new JsonReader(json);
        Map<String, String> map = reader.readObjectAsStringMap();
        reader.skipWhitespace();
        if (!reader.eof()) {
            throw new IllegalArgumentException("Trailing JSON content");
        }
        return Map.copyOf(map);
    }

    private static void writeValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
            return;
        }
        if (value instanceof String s) {
            writeString(sb, s);
            return;
        }
        if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
            return;
        }
        if (value instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeString(sb, String.valueOf(entry.getKey()));
                sb.append(':');
                writeValue(sb, entry.getValue());
            }
            sb.append('}');
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported outbox JSON type: " + value.getClass().getName());
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
    }

    private static final class JsonReader {
        private final String json;
        private int i;

        JsonReader(String json) {
            this.json = Objects.requireNonNull(json, "json");
        }

        boolean eof() {
            return i >= json.length();
        }

        void skipWhitespace() {
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
                i++;
            }
        }

        Map<String, String> readObjectAsStringMap() {
            skipWhitespace();
            expect('{');
            Map<String, String> map = new LinkedHashMap<>();
            skipWhitespace();
            if (peek() == '}') {
                i++;
                return map;
            }
            while (true) {
                skipWhitespace();
                String key = readString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                String value = readJsonValueAsString();
                map.put(key, value);
                skipWhitespace();
                char c = next();
                if (c == '}') {
                    return map;
                }
                if (c != ',') {
                    throw new IllegalArgumentException("Expected comma in JSON object");
                }
            }
        }

        private String readJsonValueAsString() {
            char c = peek();
            if (c == '"') {
                return readString();
            }
            if (c == 'n' && json.startsWith("null", i)) {
                i += 4;
                return null;
            }
            if (c == 't' && json.startsWith("true", i)) {
                i += 4;
                return "true";
            }
            if (c == 'f' && json.startsWith("false", i)) {
                i += 5;
                return "false";
            }
            if (c == '-' || Character.isDigit(c)) {
                int start = i;
                if (c == '-') {
                    i++;
                }
                while (i < json.length() && (Character.isDigit(json.charAt(i)) || json.charAt(i) == '.')) {
                    i++;
                }
                return json.substring(start, i);
            }
            throw new IllegalArgumentException("Unsupported JSON value at index " + i);
        }

        private String readString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (i < json.length()) {
                char c = next();
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (eof()) {
                        throw new IllegalArgumentException("Unterminated escape");
                    }
                    char escaped = next();
                    sb.append(
                            switch (escaped) {
                                case '"' -> '"';
                                case '\\' -> '\\';
                                case 'n' -> '\n';
                                case 'r' -> '\r';
                                case 't' -> '\t';
                                default -> escaped;
                            });
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated JSON string");
        }

        private char peek() {
            if (eof()) {
                throw new IllegalArgumentException("Unexpected end of JSON");
            }
            return json.charAt(i);
        }

        private char next() {
            char c = peek();
            i++;
            return c;
        }

        private void expect(char expected) {
            char c = next();
            if (c != expected) {
                throw new IllegalArgumentException("Expected '" + expected + "' but was '" + c + "'");
            }
        }
    }
}
