package com.kholodilin.outbox.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.sql.DataSource;

import com.kholodilin.outbox.channel.OutboxChannelProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates or validates partitioned outbox tables per channel.
 */
public final class OutboxSchemaManager {

    private static final Logger log = LoggerFactory.getLogger(OutboxSchemaManager.class);
    private static final String DDL_RESOURCE = "/outbox-schema.sql";

    private final DataSource dataSource;

    public OutboxSchemaManager(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public void apply(String tableName, OutboxChannelProperties.SchemaMode mode) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(mode, "mode");
        if (mode == OutboxChannelProperties.SchemaMode.NONE) {
            return;
        }
        boolean exists = tableExists(tableName);
        if (mode == OutboxChannelProperties.SchemaMode.VALIDATE) {
            if (!exists) {
                throw new IllegalStateException(
                        "Outbox table '" + tableName + "' does not exist (schema.mode=validate)");
            }
            return;
        }
        if (mode == OutboxChannelProperties.SchemaMode.CREATE && exists) {
            log.debug("Outbox table {} already exists", tableName);
            return;
        }
        String ddl = loadDdl().replace("${table_name}", tableName);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            for (String statementSql : ddl.split(";")) {
                String sql = statementSql.trim();
                if (!sql.isEmpty()) {
                    statement.execute(sql);
                }
            }
            log.info("Created outbox schema for table {}", tableName);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to create outbox schema for " + tableName, ex);
        }
    }

    private boolean tableExists(String tableName) {
        try (Connection connection = dataSource.getConnection();
                ResultSet rs = connection.getMetaData().getTables(null, null, tableName, new String[] {"TABLE"})) {
            if (rs.next()) {
                return true;
            }
            try (ResultSet rs2 =
                    connection.getMetaData().getTables(null, null, tableName.toLowerCase(), new String[] {"TABLE"})) {
                return rs2.next();
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to check outbox table " + tableName, ex);
        }
    }

    private static String loadDdl() {
        try (InputStream in = OutboxSchemaManager.class.getResourceAsStream(DDL_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + DDL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load " + DDL_RESOURCE, ex);
        }
    }
}
