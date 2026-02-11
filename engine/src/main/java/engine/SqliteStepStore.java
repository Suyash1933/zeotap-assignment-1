package engine;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.sql.DataSource;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.sqlite.SQLiteOpenMode;

public final class SqliteStepStore implements StepStore {
    private static final int DEFAULT_BUSY_RETRIES = 8;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 40L;
    private static final int SQLITE_BUSY_TIMEOUT_MS = 5_000;

    private final DataSource dataSource;
    private final int busyRetries;
    private final long retryBackoffMs;

    public SqliteStepStore(Path dbPath) {
        this("jdbc:sqlite:" + dbPath.toAbsolutePath(), DEFAULT_BUSY_RETRIES, DEFAULT_RETRY_BACKOFF_MS);
    }

    public SqliteStepStore(String jdbcUrl, int busyRetries, long retryBackoffMs) {
        this.dataSource = createDataSource(Objects.requireNonNull(jdbcUrl, "jdbcUrl"));
        this.busyRetries = busyRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    public void initialize() throws SQLException {
        executeWithBusyRetry(() -> {
            try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS steps (
                            workflow_id TEXT NOT NULL,
                            step_key TEXT NOT NULL,
                            step_id TEXT NOT NULL,
                            status TEXT NOT NULL,
                            output_json TEXT,
                            output_type TEXT,
                            error_message TEXT,
                            attempt INTEGER NOT NULL DEFAULT 0,
                            owner TEXT,
                            started_at_ms INTEGER NOT NULL,
                            updated_at_ms INTEGER NOT NULL,
                            PRIMARY KEY (workflow_id, step_key)
                        ) WITHOUT ROWID
                        """);
                statement.execute("""
                        CREATE INDEX IF NOT EXISTS idx_steps_workflow_status
                        ON steps (workflow_id, status)
                        """);
            }
            return null;
        });
    }

    @Override
    public StepReservation reserve(String workflowId, String stepKey, String stepId, String owner, long leaseMs)
            throws SQLException {
        return executeWithBusyRetry(() -> {
            try (Connection connection = openConnection()) {
                beginImmediate(connection);
                try {
                    StepRecord existing = selectStep(connection, workflowId, stepKey);
                    long now = System.currentTimeMillis();

                    if (existing == null) {
                        insertRunning(connection, workflowId, stepKey, stepId, owner, now);
                        commit(connection);
                        return StepReservation.acquired(
                                new StepRecord(workflowId, stepKey, stepId, StepStatus.RUNNING,
                                        null, null, null, 1, owner, now, now));
                    }

                    if (existing.status() == StepStatus.COMPLETED) {
                        commit(connection);
                        return StepReservation.cached(existing);
                    }

                    if (existing.status() == StepStatus.RUNNING) {
                        boolean isStale = now - existing.updatedAtMs() > leaseMs;
                        boolean sameOwner = owner.equals(existing.owner());
                        if (!isStale && !sameOwner) {
                            commit(connection);
                            return StepReservation.runningElsewhere(existing);
                        }
                    }

                    int nextAttempt = existing.attempt() + 1;
                    updateToRunning(connection, workflowId, stepKey, owner, now, nextAttempt);
                    commit(connection);
                    return StepReservation.acquired(
                            new StepRecord(workflowId, stepKey, stepId, StepStatus.RUNNING,
                                    null, null, null, nextAttempt, owner,
                                    existing.startedAtMs() > 0 ? existing.startedAtMs() : now, now));
                } catch (SQLException e) {
                    rollbackQuietly(connection);
                    throw e;
                }
            }
        });
    }

    @Override
    public void complete(String workflowId, String stepKey, String owner, String outputJson, String outputType)
            throws SQLException {
        executeWithBusyRetry(() -> {
            long now = System.currentTimeMillis();
            try (Connection connection = openConnection();
                 PreparedStatement statement = connection.prepareStatement("""
                         UPDATE steps
                         SET status = ?,
                             output_json = ?,
                             output_type = ?,
                             error_message = NULL,
                             updated_at_ms = ?
                         WHERE workflow_id = ?
                           AND step_key = ?
                           AND owner = ?
                         """)) {
                statement.setString(1, StepStatus.COMPLETED.name());
                statement.setString(2, outputJson);
                statement.setString(3, outputType);
                statement.setLong(4, now);
                statement.setString(5, workflowId);
                statement.setString(6, stepKey);
                statement.setString(7, owner);
                int changed = statement.executeUpdate();
                if (changed == 0) {
                    throw new SQLException("Could not mark step completed. Ownership was lost for " + stepKey);
                }
            }
            return null;
        });
    }

    @Override
    public void fail(String workflowId, String stepKey, String owner, String errorMessage) throws SQLException {
        executeWithBusyRetry(() -> {
            long now = System.currentTimeMillis();
            try (Connection connection = openConnection();
                 PreparedStatement statement = connection.prepareStatement("""
                         UPDATE steps
                         SET status = ?,
                             error_message = ?,
                             updated_at_ms = ?
                         WHERE workflow_id = ?
                           AND step_key = ?
                           AND owner = ?
                         """)) {
                statement.setString(1, StepStatus.FAILED.name());
                statement.setString(2, errorMessage);
                statement.setLong(3, now);
                statement.setString(4, workflowId);
                statement.setString(5, stepKey);
                statement.setString(6, owner);
                int changed = statement.executeUpdate();
                if (changed == 0) {
                    throw new SQLException("Could not mark step failed. Ownership was lost for " + stepKey);
                }
            }
            return null;
        });
    }

    private StepRecord selectStep(Connection connection, String workflowId, String stepKey) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT workflow_id,
                       step_key,
                       step_id,
                       status,
                       output_json,
                       output_type,
                       error_message,
                       attempt,
                       owner,
                       started_at_ms,
                       updated_at_ms
                FROM steps
                WHERE workflow_id = ?
                  AND step_key = ?
                """)) {
            statement.setString(1, workflowId);
            statement.setString(2, stepKey);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapRecord(rs);
            }
        }
    }

    private void insertRunning(Connection connection, String workflowId, String stepKey, String stepId,
                               String owner, long now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO steps (
                    workflow_id,
                    step_key,
                    step_id,
                    status,
                    attempt,
                    owner,
                    started_at_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, workflowId);
            statement.setString(2, stepKey);
            statement.setString(3, stepId);
            statement.setString(4, StepStatus.RUNNING.name());
            statement.setInt(5, 1);
            statement.setString(6, owner);
            statement.setLong(7, now);
            statement.setLong(8, now);
            statement.executeUpdate();
        }
    }

    private void updateToRunning(Connection connection, String workflowId, String stepKey,
                                 String owner, long now, int attempt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE steps
                SET status = ?,
                    owner = ?,
                    output_json = NULL,
                    output_type = NULL,
                    error_message = NULL,
                    attempt = ?,
                    updated_at_ms = ?
                WHERE workflow_id = ?
                  AND step_key = ?
                """)) {
            statement.setString(1, StepStatus.RUNNING.name());
            statement.setString(2, owner);
            statement.setInt(3, attempt);
            statement.setLong(4, now);
            statement.setString(5, workflowId);
            statement.setString(6, stepKey);
            statement.executeUpdate();
        }
    }

    private StepRecord mapRecord(ResultSet rs) throws SQLException {
        return new StepRecord(
                rs.getString("workflow_id"),
                rs.getString("step_key"),
                rs.getString("step_id"),
                StepStatus.valueOf(rs.getString("status")),
                rs.getString("output_json"),
                rs.getString("output_type"),
                rs.getString("error_message"),
                rs.getInt("attempt"),
                rs.getString("owner"),
                rs.getLong("started_at_ms"),
                rs.getLong("updated_at_ms"));
    }

    private Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private static void beginImmediate(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
        }
    }

    private static void commit(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("COMMIT");
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // Best effort rollback.
        }
    }

    private <T> T executeWithBusyRetry(SqlSupplier<T> supplier) throws SQLException {
        SQLException last = null;
        for (int attempt = 0; attempt <= busyRetries; attempt++) {
            try {
                return supplier.get();
            } catch (SQLException e) {
                if (!isBusy(e) || attempt == busyRetries) {
                    throw e;
                }
                last = e;
                sleep(retryBackoffMs * (attempt + 1));
            }
        }
        throw last;
    }

    private boolean isBusy(SQLException e) {
        if (e instanceof SQLiteException sqliteException) {
            SQLiteErrorCode resultCode = sqliteException.getResultCode();
            if (resultCode == SQLiteErrorCode.SQLITE_BUSY || resultCode == SQLiteErrorCode.SQLITE_LOCKED) {
                return true;
            }
        }
        String message = e.getMessage();
        return e.getErrorCode() == 5
                || (message != null
                && (message.contains("SQLITE_BUSY")
                || message.contains("SQLITE_LOCKED")
                || message.contains("database is locked")));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying SQLite busy operation", interrupted);
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {
        T get() throws SQLException;
    }

    private static DataSource createDataSource(String jdbcUrl) {
        SQLiteConfig config = new SQLiteConfig();
        config.setOpenMode(SQLiteOpenMode.FULLMUTEX);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        config.setBusyTimeout(SQLITE_BUSY_TIMEOUT_MS);

        SQLiteDataSource sqliteDataSource = new SQLiteDataSource(config);
        sqliteDataSource.setUrl(jdbcUrl);
        return sqliteDataSource;
    }
}
