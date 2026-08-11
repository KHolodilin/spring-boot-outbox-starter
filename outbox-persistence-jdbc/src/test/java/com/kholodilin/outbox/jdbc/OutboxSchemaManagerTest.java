package com.kholodilin.outbox.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

import com.kholodilin.outbox.channel.OutboxChannelProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxSchemaManagerTest {

    @Test
    void noneModeIsNoOp() {
        DataSource ds = mock(DataSource.class);
        new OutboxSchemaManager(ds).apply("outbox_events", OutboxChannelProperties.SchemaMode.NONE);
    }

    @Test
    void validateFailsWhenMissing() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        when(ds.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(meta);
        when(meta.getTables(any(), any(), anyString(), any())).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        assertThatThrownBy(() ->
                        new OutboxSchemaManager(ds).apply("outbox_events", OutboxChannelProperties.SchemaMode.VALIDATE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void createExecutesDdl() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet rs = mock(ResultSet.class);
        Statement statement = mock(Statement.class);
        when(ds.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(meta);
        when(meta.getTables(any(), any(), anyString(), any())).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        new OutboxSchemaManager(ds).apply("outbox_events", OutboxChannelProperties.SchemaMode.CREATE);
        verify(statement, org.mockito.Mockito.atLeastOnce())
                .execute(org.mockito.ArgumentMatchers.contains("CREATE TABLE"));
    }
}
