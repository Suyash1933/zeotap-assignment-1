package engine;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

public final class DurableEngine {
    private static final long DEFAULT_LEASE_MS = 3_000L;

    private final StepStore stepStore;
    private final JsonCodec jsonCodec;
    private final long leaseMs;
    private final String workerId;
    private final CrashConfig crashConfig;

    public DurableEngine(StepStore stepStore) {
        this(stepStore, DEFAULT_LEASE_MS, "worker-" + UUID.randomUUID(), CrashConfig.NONE);
    }

    public DurableEngine(StepStore stepStore, long leaseMs, String workerId, CrashConfig crashConfig) {
        this.stepStore = Objects.requireNonNull(stepStore, "stepStore");
        this.jsonCodec = new JsonCodec();
        this.leaseMs = leaseMs;
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.crashConfig = Objects.requireNonNull(crashConfig, "crashConfig");
    }

    public static DurableEngine forSqlite(Path dbPath, CrashConfig crashConfig) throws SQLException {
        SqliteStepStore store = new SqliteStepStore(dbPath);
        store.initialize();
        return new DurableEngine(store, DEFAULT_LEASE_MS, "worker-" + UUID.randomUUID(), crashConfig);
    }

    public static DurableEngine forSqlite(Path dbPath) throws SQLException {
        return forSqlite(dbPath, CrashConfig.NONE);
    }

    public <T> T run(String workflowId, Workflow<T> workflow) throws Exception {
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(workflow, "workflow");
        DurableContext context = new DurableContext(workflowId, stepStore, jsonCodec, leaseMs, workerId, crashConfig);
        return workflow.run(context);
    }
}
