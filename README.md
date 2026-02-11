# Native Durable Execution Engine (Java)

This repository implements Assignment 1: a durable workflow engine where workflow code is written in normal Java and becomes crash-resumable through a `step(...)` primitive.

## Repository Structure

- `engine/`: core durable execution library (`DurableEngine`, `DurableContext`, SQLite-backed `StepStore`)
- `examples/onboarding/`: sample Employee Onboarding workflow
- `app/`: CLI app to run/resume workflow and inject simulated crashes
- `Prompts.txt`: prompts used during AI-assisted implementation

## What Is Implemented

### Challenge Coverage

- Step primitive for side effects: implemented via `DurableContext.step(...)` and `stepAsync(...)` in `engine/src/main/java/engine/DurableContext.java`.
- Memoization with RDBMS check before execution: implemented in `SqliteStepStore.reserve(...)` returning cached completed outputs from `steps` table.
- Sequence management for loops/conditionals with repeated step IDs: implemented by generated `step_key` using `stepId + callsiteHash + perCallsiteSequence`.

### Requirements Checklist

Functional requirements:

- Workflow runner (start/resume): `DurableEngine.run(...)` in `engine/src/main/java/engine/DurableEngine.java`.
- Generic step primitive: `<T> T step(String id, Callable<T> fn)` in `engine/src/main/java/engine/DurableContext.java`.
- Resilience on failure/crash:
  - step failures mark `FAILED` and stop workflow via exception propagation.
  - reruns skip completed steps by cached replay from `steps`.
- Concurrency with thread-safe SQLite writes:
  - parallel execution via `CompletableFuture` in `examples/onboarding/src/main/java/examples/onboarding/EmployeeOnboardingWorkflow.java`.
  - SQLite safety via WAL mode, busy timeout, busy retries, and transactional reservation in `engine/src/main/java/engine/SqliteStepStore.java`.

Persistence requirements:

- RDBMS state store: SQLite is used in engine and example services.
- `steps` table includes required fields:
  - `workflow_id`
  - `step_key` (ID + sequence based key)
  - `status`
  - serialized output (`output_json`, plus `output_type` for typed deserialization)

## Concept Examples

### Durable execution

Run once with a workflow ID:

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-durable-1 --employee-id emp-501 --name 'Durable User' --email durable@example.com --db durable.db"
```

Run again with the same `workflow-id` and `db`: completed steps are replayed from stored results instead of re-running side effects.

### Crash recovery

Simulate a crash between side effect execution and step commit:

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-crash-1 --employee-id emp-502 --name 'Crash User' --email crash@example.com --db durable.db --crash-step provision-laptop --crash-phase after-execute-before-commit"
```

Resume with the same workflow:

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-crash-1 --employee-id emp-502 --name 'Crash User' --email crash@example.com --db durable.db"
```

### Deterministic replay

Use deterministic control-flow inputs (from request/state), then place side effects behind `step(...)`.

```java
boolean needsManagerAccess = request.role().equals("manager"); // deterministic input
if (needsManagerAccess) {
    context.step("grant-manager-access", () -> grantManagerAccess(request.employeeId()));
}
```

Avoid non-deterministic branching outside steps (for example `if (System.currentTimeMillis() % 2 == 0)`), because replay path can diverge.

### Idempotency

The onboarding example uses an idempotency key for API-like calls:

```java
String laptopId = callIdempotentApi(
    "provision-laptop",
    "laptop:" + employeeId,
    "{\"employeeId\":\"" + employeeId + "\"}");
```

`api_call_log` stores `idempotency_key -> external_ref`, so retries reuse the same external side effect result.

### Exactly-once semantics

The engine provides effectively-once behavior for step outcomes within a workflow instance:

- Step result is persisted in `steps`.
- Future replays for the same `workflow_id + step_key` return cached result.
- Ownership checks prevent duplicate commit races.

For external systems, combine durable steps with idempotency keys (as done in `api_call_log`) to prevent duplicate real-world effects.

### Logical clocks

Repeated step IDs are disambiguated by sequence per callsite:

```java
for (int i = 0; i < 3; i++) {
    context.step("notify", () -> sendNotification(i));
}
```

This generates distinct step keys such as:

- `notify::<callsiteHash>::1`
- `notify::<callsiteHash>::2`
- `notify::<callsiteHash>::3`

### 1. Durable Step Primitive

Core API (in `engine/DurableContext.java`):

- `<T> T step(String id, Callable<T> fn)`
- `<T> T step(Callable<T> fn)` (automatic ID generation)
- `<T> CompletableFuture<T> stepAsync(String id, Callable<T> fn, Executor executor)`

Every step is checkpointed in SQLite. If a completed row exists for the same workflow + step key, cached output is returned and side effects are skipped.

### 2. Persistence Layer (RDBMS)

`engine/SqliteStepStore.java` creates and uses:

- `steps(workflow_id, step_key, step_id, status, output_json, output_type, error_message, attempt, owner, started_at_ms, updated_at_ms)`
- Primary key: `(workflow_id, step_key)`

Step statuses:

- `RUNNING`
- `COMPLETED`
- `FAILED`

### 3. Sequence Tracking (Loops + Conditionals)

Step keys are generated as:

- `step_key = stepId + callsiteHash + perCallsiteSequence`

Where:

- `callsiteHash` comes from stack frame location (class/method/line)
- `perCallsiteSequence` is an atomic counter per `(stepId, callsite)`

This allows repeated invocations (such as in loops) to map to stable unique keys without forcing manual unique IDs each time.

### 4. Concurrency and Thread Safety

Parallel steps are supported via `CompletableFuture`.

Thread safety guarantees:

- SQLite configured with `PRAGMA journal_mode=WAL` and `busy_timeout`
- retries with backoff on `SQLITE_BUSY` / `database is locked`
- transaction `BEGIN IMMEDIATE` used during step reservation
- ownership (`owner`) and lease checks prevent conflicting concurrent workers from executing the same step at once

### 5. Resilience + Zombie Step Handling

If a step fails, workflow stops and step is marked `FAILED`.

On rerun with the same `workflowId`:

- completed steps return cached outputs
- failed or stale-running steps are retried
- running-step lease defaults to 3 seconds, so crash recovery can reclaim orphaned `RUNNING` steps quickly

Zombie-step risk (crash after side effect but before step commit) is mitigated in the example by idempotency keys for API-like calls:

- `api_call_log` table keyed by `idempotency_key`
- repeated API attempts return same external reference

### 6. Assignment Workflow Example

`examples/onboarding/EmployeeOnboardingWorkflow.java` implements:

1. Step 1 (sequential): create employee record (DB write)
2. Step 2 + 3 (parallel): provision laptop + provision access (API simulation + DB writes)
3. Step 4 (sequential): send welcome email (API simulation + DB write)

## Build and Run

### Prerequisites

- Java 17+
- Maven 3.9+

### Build

```bash
mvn clean package
```

### Run CLI

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-001 --employee-id emp-001 --name 'Ada Lovelace' --email ada@example.com --db durable.db"
```

## Crash and Resume Demo

### 1. Simulate crash after side effect and before commit

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-001 --employee-id emp-001 --name 'Ada Lovelace' --email ada@example.com --db durable.db --crash-step provision-laptop --crash-phase after-execute-before-commit"
```

Process halts intentionally.

### 2. Resume same workflow

```bash
mvn -q -pl app exec:java -Dexec.args="--workflow-id wf-001 --employee-id emp-001 --name 'Ada Lovelace' --email ada@example.com --db durable.db"
```

Expected behavior:

- previously completed steps are skipped (loaded from `steps` table)
- in-progress/failed step resumes
- idempotent API simulation avoids duplicated external side effects

## Loop Example (How Sequence IDs Work)

```java
for (int i = 0; i < 3; i++) {
    context.step("loop-step", () -> doSideEffect(i));
}
```

This produces three distinct checkpointed step keys for the same `stepId`, because the per-callsite sequence counter increments each invocation.

## Notes

- The engine API remains idiomatic Java (`Callable`, `CompletableFuture`, generics), with no DSL.
- Results are serialized to JSON using Jackson.
- SQLite is used as the durable state store for both engine checkpoints and onboarding sample side effects.

## SQLite Performance Configuration

JDBC configuration used by the engine (`engine/src/main/java/engine/SqliteStepStore.java`):

```java
SQLiteConfig config = new SQLiteConfig();
config.setJournalMode(SQLiteConfig.JournalMode.WAL);
config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
config.setBusyTimeout(5_000);

SQLiteDataSource dataSource = new SQLiteDataSource(config);
dataSource.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath());
```

Updated schema:

```sql
CREATE TABLE IF NOT EXISTS steps (
    workflow_id   TEXT NOT NULL,
    step_key      TEXT NOT NULL,
    step_id       TEXT NOT NULL,
    status        TEXT NOT NULL,
    output_json   TEXT,
    output_type   TEXT,
    error_message TEXT,
    attempt       INTEGER NOT NULL DEFAULT 0,
    owner         TEXT,
    started_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, step_key)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_steps_workflow_status
ON steps (workflow_id, status);
```

`(workflow_id, step_key)` lookup is indexed via the composite primary key. Also, external step work (`fn.call()`) is executed outside DB transactions in `DurableContext.stepWithKey(...)`, so locks are not held while calling APIs or running side effects.
