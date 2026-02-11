package examples.onboarding;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public final class OnboardingServices {
    private static final int DEFAULT_BUSY_RETRIES = 8;
    private static final long RETRY_BACKOFF_MS = 35L;

    private final String jdbcUrl;

    public OnboardingServices(Path dbPath) {
        this.jdbcUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
    }

    public void initialize() throws SQLException {
        withBusyRetry(() -> {
            try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS employees (
                            employee_id TEXT PRIMARY KEY,
                            full_name TEXT NOT NULL,
                            email TEXT NOT NULL,
                            created_at_ms INTEGER NOT NULL,
                            updated_at_ms INTEGER NOT NULL
                        )
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS onboarding_assets (
                            employee_id TEXT PRIMARY KEY,
                            laptop_id TEXT,
                            access_id TEXT,
                            updated_at_ms INTEGER NOT NULL
                        )
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS sent_emails (
                            employee_id TEXT PRIMARY KEY,
                            email TEXT NOT NULL,
                            message_id TEXT NOT NULL,
                            sent_at_ms INTEGER NOT NULL
                        )
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS api_call_log (
                            idempotency_key TEXT PRIMARY KEY,
                            endpoint TEXT NOT NULL,
                            external_ref TEXT NOT NULL,
                            payload TEXT,
                            created_at_ms INTEGER NOT NULL
                        )
                        """);
            }
            return null;
        });
    }

    public EmployeeRecord createEmployeeRecord(OnboardingRequest request) throws SQLException {
        long now = System.currentTimeMillis();
        return withBusyRetry(() -> {
            try (Connection connection = openConnection()) {
                try (PreparedStatement upsert = connection.prepareStatement("""
                        INSERT INTO employees (employee_id, full_name, email, created_at_ms, updated_at_ms)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(employee_id) DO UPDATE SET
                            full_name = excluded.full_name,
                            email = excluded.email,
                            updated_at_ms = excluded.updated_at_ms
                        """)) {
                    upsert.setString(1, request.employeeId());
                    upsert.setString(2, request.fullName());
                    upsert.setString(3, request.email());
                    upsert.setLong(4, now);
                    upsert.setLong(5, now);
                    upsert.executeUpdate();
                }

                try (PreparedStatement select = connection.prepareStatement("""
                        SELECT employee_id, full_name, created_at_ms
                        FROM employees
                        WHERE employee_id = ?
                        """)) {
                    select.setString(1, request.employeeId());
                    try (ResultSet rs = select.executeQuery()) {
                        if (!rs.next()) {
                            throw new SQLException("Employee row missing after upsert for " + request.employeeId());
                        }
                        return new EmployeeRecord(
                                rs.getString("employee_id"),
                                rs.getString("full_name"),
                                rs.getLong("created_at_ms"));
                    }
                }
            }
        });
    }

    public ProvisionResponse provisionLaptop(String employeeId) throws SQLException {
        String laptopId = callIdempotentApi(
                "provision-laptop",
                "laptop:" + employeeId,
                "{\"employeeId\":\"" + employeeId + "\"}");

        upsertLaptop(employeeId, laptopId);
        return new ProvisionResponse(laptopId, "PROVISIONED");
    }

    public ProvisionResponse provisionAccess(String employeeId) throws SQLException {
        String accessId = callIdempotentApi(
                "provision-access",
                "access:" + employeeId,
                "{\"employeeId\":\"" + employeeId + "\"}");

        upsertAccess(employeeId, accessId);
        return new ProvisionResponse(accessId, "PROVISIONED");
    }

    public EmailReceipt sendWelcomeEmail(String employeeId, String email) throws SQLException {
        String messageId = callIdempotentApi(
                "send-welcome-email",
                "email:" + employeeId,
                "{\"employeeId\":\"" + employeeId + "\",\"email\":\"" + email + "\"}");

        long now = System.currentTimeMillis();
        withBusyRetry(() -> {
            try (Connection connection = openConnection();
                 PreparedStatement upsert = connection.prepareStatement("""
                         INSERT INTO sent_emails (employee_id, email, message_id, sent_at_ms)
                         VALUES (?, ?, ?, ?)
                         ON CONFLICT(employee_id) DO UPDATE SET
                             email = excluded.email,
                             message_id = excluded.message_id,
                             sent_at_ms = excluded.sent_at_ms
                         """)) {
                upsert.setString(1, employeeId);
                upsert.setString(2, email);
                upsert.setString(3, messageId);
                upsert.setLong(4, now);
                upsert.executeUpdate();
            }
            return null;
        });

        return new EmailReceipt(messageId, "SENT");
    }

    private void upsertLaptop(String employeeId, String laptopId) throws SQLException {
        long now = System.currentTimeMillis();
        withBusyRetry(() -> {
            try (Connection connection = openConnection();
                 PreparedStatement upsert = connection.prepareStatement("""
                         INSERT INTO onboarding_assets (employee_id, laptop_id, access_id, updated_at_ms)
                         VALUES (?, ?, NULL, ?)
                         ON CONFLICT(employee_id) DO UPDATE SET
                             laptop_id = excluded.laptop_id,
                             updated_at_ms = excluded.updated_at_ms
                         """)) {
                upsert.setString(1, employeeId);
                upsert.setString(2, laptopId);
                upsert.setLong(3, now);
                upsert.executeUpdate();
            }
            return null;
        });
    }

    private void upsertAccess(String employeeId, String accessId) throws SQLException {
        long now = System.currentTimeMillis();
        withBusyRetry(() -> {
            try (Connection connection = openConnection();
                 PreparedStatement upsert = connection.prepareStatement("""
                         INSERT INTO onboarding_assets (employee_id, laptop_id, access_id, updated_at_ms)
                         VALUES (?, NULL, ?, ?)
                         ON CONFLICT(employee_id) DO UPDATE SET
                             access_id = excluded.access_id,
                             updated_at_ms = excluded.updated_at_ms
                         """)) {
                upsert.setString(1, employeeId);
                upsert.setString(2, accessId);
                upsert.setLong(3, now);
                upsert.executeUpdate();
            }
            return null;
        });
    }

    private String callIdempotentApi(String endpoint, String idempotencyKey, String payload) throws SQLException {
        simulateApiLatency(endpoint);
        String generatedExternalRef = generateExternalRef(endpoint, idempotencyKey);
        long now = System.currentTimeMillis();

        return withBusyRetry(() -> {
            try (Connection connection = openConnection()) {
                try (PreparedStatement insert = connection.prepareStatement("""
                        INSERT INTO api_call_log (idempotency_key, endpoint, external_ref, payload, created_at_ms)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(idempotency_key) DO NOTHING
                        """)) {
                    insert.setString(1, idempotencyKey);
                    insert.setString(2, endpoint);
                    insert.setString(3, generatedExternalRef);
                    insert.setString(4, payload);
                    insert.setLong(5, now);
                    insert.executeUpdate();
                }

                try (PreparedStatement select = connection.prepareStatement("""
                        SELECT external_ref
                        FROM api_call_log
                        WHERE idempotency_key = ?
                        """)) {
                    select.setString(1, idempotencyKey);
                    try (ResultSet rs = select.executeQuery()) {
                        if (!rs.next()) {
                            throw new SQLException("Idempotent API log missing for key " + idempotencyKey);
                        }
                        return rs.getString("external_ref");
                    }
                }
            }
        });
    }

    private static String generateExternalRef(String endpoint, String idempotencyKey) {
        String prefix = switch (endpoint.toLowerCase(Locale.ROOT)) {
            case "provision-laptop" -> "LAP";
            case "provision-access" -> "ACC";
            case "send-welcome-email" -> "MSG";
            default -> "EXT";
        };
        return prefix + "-" + shortHash(idempotencyKey);
    }

    private static String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                builder.append(String.format("%02x", bytes[i]));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static void simulateApiLatency(String endpoint) {
        int min;
        int max;
        if ("send-welcome-email".equals(endpoint)) {
            min = 40;
            max = 140;
        } else {
            min = 140;
            max = 350;
        }

        int sleepMs = ThreadLocalRandom.current().nextInt(min, max + 1);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during simulated API call", interrupted);
        }
    }

    private Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=NORMAL");
            statement.execute("PRAGMA busy_timeout=5000");
        }
        return connection;
    }

    private <T> T withBusyRetry(SqlSupplier<T> supplier) throws SQLException {
        SQLException last = null;
        for (int attempt = 0; attempt <= DEFAULT_BUSY_RETRIES; attempt++) {
            try {
                return supplier.run();
            } catch (SQLException e) {
                if (!isBusy(e) || attempt == DEFAULT_BUSY_RETRIES) {
                    throw e;
                }
                last = e;
                sleep(RETRY_BACKOFF_MS * (attempt + 1));
            }
        }
        throw last;
    }

    private static boolean isBusy(SQLException e) {
        String message = e.getMessage();
        return e.getErrorCode() == 5
                || (message != null
                && (message.contains("SQLITE_BUSY") || message.contains("database is locked")));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying SQLite operation", interrupted);
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {
        T run() throws SQLException;
    }
}
