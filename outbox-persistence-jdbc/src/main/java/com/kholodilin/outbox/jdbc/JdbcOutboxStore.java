package com.kholodilin.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.spi.OutboxStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * PostgreSQL JDBC {@link OutboxStore} for a single channel table.
 */
public final class JdbcOutboxStore implements OutboxStore {

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private final String channelName;
    private final JsonMapper jsonMapper;
    private final RowMapper<OutboxRecord> rowMapper;

    public JdbcOutboxStore(JdbcTemplate jdbcTemplate, String tableName, String channelName, JsonMapper jsonMapper) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate");
        this.tableName = validateTableName(tableName);
        this.channelName = Objects.requireNonNull(channelName, "channelName");
        this.jsonMapper = jsonMapper == null ? JsonMapper.builder().build() : jsonMapper;
        this.rowMapper = this::mapRow;
    }

    @Override
    public long insert(OutboxInsert insert) {
        String headersJson = writeHeaders(insert.headers());
        Long id = jdbcTemplate.queryForObject(
                """
                INSERT INTO %s (aggregate_id, partition_key, event_type, payload, headers, status, retry_count, trace_parent, created_at)
                VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?, 0, ?, NOW())
                RETURNING id
                """.formatted(tableName),
                Long.class,
                insert.aggregateId(),
                insert.partitionKey(),
                insert.eventType(),
                insert.payloadJson(),
                headersJson,
                OutboxStatus.NEW.getCode(),
                insert.traceParent());
        return Objects.requireNonNull(id, "generated id");
    }

    @Override
    public List<OutboxRecord> claimByIds(List<Long> eventIds, String lockedBy, Instant lockedUntil) {
        if (eventIds == null || eventIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(eventIds.size(), "?"));
        List<Object> params = new ArrayList<>();
        params.add(OutboxStatus.PROCESSING.getCode());
        params.add(lockedBy);
        params.add(Timestamp.from(lockedUntil));
        params.addAll(eventIds);
        params.add(OutboxStatus.ARCHIVE_THRESHOLD);

        return jdbcTemplate.query("""
                UPDATE %s
                SET status = ?, locked_by = ?, locked_until = ?
                WHERE id IN (%s)
                  AND status < ?
                  AND (locked_until IS NULL OR locked_until < NOW())
                RETURNING %s
                """.formatted(tableName, placeholders, columns()), rowMapper, params.toArray());
    }

    @Override
    public void markSent(List<Long> eventIds, Instant sentAt) {
        if (eventIds == null || eventIds.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", Collections.nCopies(eventIds.size(), "?"));
        List<Object> params = new ArrayList<>();
        params.add(OutboxStatus.SENT.getCode());
        params.add(Timestamp.from(sentAt));
        params.addAll(eventIds);
        jdbcTemplate.update("""
                UPDATE %s
                SET status = ?, sent_at = ?, locked_by = NULL, locked_until = NULL
                WHERE id IN (%s)
                """.formatted(tableName, placeholders), params.toArray());
    }

    @Override
    public void markFailed(long eventId, int retryCount, OutboxStatus status) {
        jdbcTemplate.update("""
                UPDATE %s
                SET status = ?, retry_count = ?, locked_by = NULL, locked_until = NULL
                WHERE id = ?
                """.formatted(tableName), status.getCode(), retryCount, eventId);
    }

    @Override
    public List<Long> claimRecoverableIds(int batchSize, String lockedBy, Instant lockedUntil) {
        return jdbcTemplate.query(
                """
                WITH candidates AS (
                    SELECT id
                    FROM %s
                    WHERE status < ?
                      AND (locked_until IS NULL OR locked_until < NOW())
                    ORDER BY id
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE %s AS o
                SET locked_by = ?,
                    locked_until = ?
                FROM candidates AS c
                WHERE o.id = c.id
                RETURNING o.id
                """.formatted(tableName, tableName),
                (rs, rowNum) -> rs.getLong("id"),
                OutboxStatus.ARCHIVE_THRESHOLD,
                batchSize,
                lockedBy,
                Timestamp.from(lockedUntil));
    }

    @Override
    public void clearLease(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", Collections.nCopies(eventIds.size(), "?"));
        jdbcTemplate.update("""
                UPDATE %s SET locked_by = NULL, locked_until = NULL WHERE id IN (%s)
                """.formatted(tableName, placeholders), eventIds.toArray());
    }

    @Override
    public List<Long> findReenqueueableIds(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(eventIds.size(), "?"));
        List<Object> params = new ArrayList<>(eventIds);
        params.add(OutboxStatus.ARCHIVE_THRESHOLD);
        return jdbcTemplate.query(
                """
                SELECT id FROM %s
                WHERE id IN (%s)
                  AND status < ?
                  AND (locked_until IS NULL OR locked_until < NOW())
                """.formatted(tableName, placeholders), (rs, rowNum) -> rs.getLong("id"), params.toArray());
    }

    @Override
    public List<OutboxRecord> findByIds(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(eventIds.size(), "?"));
        return jdbcTemplate.query("""
                SELECT %s FROM %s WHERE id IN (%s)
                """.formatted(columns(), tableName, placeholders), rowMapper, eventIds.toArray());
    }

    private String columns() {
        return "id, aggregate_id, partition_key, event_type, payload::text AS payload, headers::text AS headers, "
                + "status, retry_count, trace_parent, created_at";
    }

    private OutboxRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp created = rs.getTimestamp("created_at");
        return new OutboxRecord(
                channelName,
                rs.getLong("id"),
                rs.getString("event_type"),
                rs.getString("aggregate_id"),
                rs.getString("partition_key"),
                rs.getString("payload"),
                readHeaders(rs.getString("headers")),
                rs.getString("trace_parent"),
                rs.getInt("retry_count"),
                created == null ? Instant.EPOCH : created.toInstant());
    }

    private String writeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        try {
            return jsonMapper.writeValueAsString(headers);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to serialize outbox headers", ex);
        }
    }

    private Map<String, String> readHeaders(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            JsonNode node = jsonMapper.readTree(json);
            Map<String, String> map = new LinkedHashMap<>();
            node.properties()
                    .forEach(entry -> map.put(entry.getKey(), entry.getValue().asString()));
            return Map.copyOf(map);
        } catch (Exception ex) {
            return Map.of();
        }
    }

    static String validateTableName(String tableName) {
        if (tableName == null || !tableName.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid outbox table name: " + tableName);
        }
        return tableName;
    }
}
