package com.kholodilin.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcOutboxStoreTest {

    private JdbcTemplate jdbc;
    private JdbcOutboxStore store;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        store = new JdbcOutboxStore(
                jdbc, "outbox_events", "default", JsonMapper.builder().build());
    }

    @Test
    void insertReturnsId() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(11L);
        long id = store.insert(new OutboxInsert("T", "a", "p", "{}", Map.of("h", "v"), "tp"));
        assertThat(id).isEqualTo(11L);
    }

    @Test
    void claimByIdsEmptyShortCircuits() {
        assertThat(store.claimByIds(List.of(), "pod", Instant.now())).isEmpty();
        assertThat(store.findByIds(List.of())).isEmpty();
        assertThat(store.findReenqueueableIds(List.of())).isEmpty();
        store.markSent(List.of(), Instant.now());
        store.clearLease(List.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    void claimMapsRow() throws Exception {
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {
            RowMapper<OutboxRecord> mapper = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getLong("id")).thenReturn(5L);
            when(rs.getString("event_type")).thenReturn("E");
            when(rs.getString("aggregate_id")).thenReturn("a");
            when(rs.getString("partition_key")).thenReturn("p");
            when(rs.getString("payload")).thenReturn("{}");
            when(rs.getString("headers")).thenReturn("{\"k\":\"v\"}");
            when(rs.getString("trace_parent")).thenReturn(null);
            when(rs.getInt("retry_count")).thenReturn(1);
            when(rs.getTimestamp("created_at")).thenReturn(Timestamp.from(Instant.parse("2020-01-01T00:00:00Z")));
            return List.of(mapper.mapRow(rs, 0));
        });

        List<OutboxRecord> claimed =
                store.claimByIds(List.of(5L), "pod", Instant.now().plusSeconds(1));
        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().eventId()).isEqualTo(5L);
        assertThat(claimed.getFirst().headers()).containsEntry("k", "v");
    }

    @Test
    void markFailedAndRecoverable() {
        store.markFailed(3L, 2, OutboxStatus.FAILED);
        verify(jdbc).update(anyString(), eq(OutboxStatus.FAILED.getCode()), eq(2), eq(3L));

        when(jdbc.query(anyString(), any(RowMapper.class), any(), any(), any(), any()))
                .thenReturn(List.of(9L));
        assertThat(store.claimRecoverableIds(10, "pod", Instant.now())).containsExactly(9L);
    }

    @Test
    void markSentAndClearLease() {
        store.markSent(List.of(1L, 2L), Instant.now());
        store.clearLease(List.of(1L));
        verify(jdbc, org.mockito.Mockito.atLeastOnce()).update(anyString(), any(Object[].class));
    }
}
