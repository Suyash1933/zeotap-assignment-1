package engine;

import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class DurableContext {
    private static final StackWalker STACK_WALKER =
            StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

    private final String workflowId;
    private final StepStore stepStore;
    private final JsonCodec jsonCodec;
    private final long leaseMs;
    private final String workerId;
    private final CrashConfig crashConfig;
    private final AtomicLong logicalClock;
    private final ConcurrentHashMap<String, AtomicInteger> callsiteSequence;
    private final ThreadLocal<String> currentStepKey;

    DurableContext(String workflowId,
                   StepStore stepStore,
                   JsonCodec jsonCodec,
                   long leaseMs,
                   String workerId,
                   CrashConfig crashConfig) {
        this.workflowId = workflowId;
        this.stepStore = stepStore;
        this.jsonCodec = jsonCodec;
        this.leaseMs = leaseMs;
        this.workerId = workerId;
        this.crashConfig = crashConfig;
        this.logicalClock = new AtomicLong(0);
        this.callsiteSequence = new ConcurrentHashMap<>();
        this.currentStepKey = new ThreadLocal<>();
    }

    public String workflowId() {
        return workflowId;
    }

    public String currentStepKey() {
        return currentStepKey.get();
    }

    public <T> T step(String id, Callable<T> fn) throws Exception {
        Objects.requireNonNull(fn, "fn");
        String stepId = normalizeStepId(id);
        String callsite = resolveCallsite();
        String stepKey = nextStepKey(stepId, callsite);
        return stepWithKey(stepId, stepKey, fn);
    }

    private <T> T stepWithKey(String stepId, String stepKey, Callable<T> fn) throws Exception {
        StepReservation reservation = reserveWithLeaseWait(stepKey, stepId);
        if (reservation.state() == ReservationState.CACHED) {
            StepRecord cached = reservation.record();
            return jsonCodec.fromJson(cached.outputJson(), cached.outputType());
        }
        if (reservation.state() == ReservationState.RUNNING_ELSEWHERE) {
            throw new StepInProgressException("Step is already running on another worker: " + stepKey);
        }

        maybeCrash(stepId, stepKey, CrashPhase.BEFORE_EXECUTE);

        T output;
        String previous = currentStepKey.get();
        currentStepKey.set(stepKey);
        try {
            output = fn.call();
        } catch (Exception e) {
            safeFail(stepKey, e);
            throw e;
        } finally {
            restoreStepKey(previous);
        }

        maybeCrash(stepId, stepKey, CrashPhase.AFTER_EXECUTE_BEFORE_COMMIT);

        String outputJson = output == null ? null : jsonCodec.toJson(output);
        String outputType = output == null ? Void.class.getName() : output.getClass().getName();
        stepStore.complete(workflowId, stepKey, workerId, outputJson, outputType);

        maybeCrash(stepId, stepKey, CrashPhase.AFTER_COMMIT);
        return output;
    }

    private StepReservation reserveWithLeaseWait(String stepKey, String stepId) throws SQLException {
        long deadline = System.currentTimeMillis() + Math.max(leaseMs, 300L);
        while (true) {
            StepReservation reservation = stepStore.reserve(workflowId, stepKey, stepId, workerId, leaseMs);
            if (reservation.state() != ReservationState.RUNNING_ELSEWHERE) {
                return reservation;
            }
            if (System.currentTimeMillis() >= deadline) {
                return reservation;
            }
            sleep(100L);
        }
    }

    public <T> T step(Callable<T> fn) throws Exception {
        Objects.requireNonNull(fn, "fn");
        String callsite = resolveCallsite();
        String stepId = autoStepId(callsite);
        String stepKey = nextStepKey(stepId, callsite);
        return stepWithKey(stepId, stepKey, fn);
    }

    public <T> CompletableFuture<T> stepAsync(String id, Callable<T> fn) {
        return stepAsync(id, fn, ForkJoinPool.commonPool());
    }

    public <T> CompletableFuture<T> stepAsync(String id, Callable<T> fn, Executor executor) {
        Objects.requireNonNull(fn, "fn");
        Objects.requireNonNull(executor, "executor");
        String stepId = normalizeStepId(id);
        String callsite = resolveCallsite();
        String stepKey = nextStepKey(stepId, callsite);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return stepWithKey(stepId, stepKey, fn);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private void maybeCrash(String stepId, String stepKey, CrashPhase phase) {
        if (crashConfig.shouldCrash(stepId, phase)) {
            System.err.printf(
                    "Simulated crash at phase=%s for stepId=%s stepKey=%s%n",
                    phase,
                    stepId,
                    stepKey);
            Runtime.getRuntime().halt(137);
        }
    }

    private void safeFail(String stepKey, Exception rootCause) {
        try {
            stepStore.fail(workflowId, stepKey, workerId, rootCause.toString());
        } catch (SQLException sqlException) {
            rootCause.addSuppressed(sqlException);
        }
    }

    private void restoreStepKey(String previous) {
        if (previous == null) {
            currentStepKey.remove();
        } else {
            currentStepKey.set(previous);
        }
    }

    private String nextStepKey(String stepId, String callsite) {
        String sequenceKey = stepId + "|" + callsite;
        long tick = logicalClock.incrementAndGet();
        int sequence = callsiteSequence
                .computeIfAbsent(sequenceKey, ignored -> new AtomicInteger(0))
                .incrementAndGet();

        return stepId
                + "::t" + Long.toUnsignedString(tick, 16)
                + "::" + Integer.toHexString(callsite.hashCode())
                + "::" + sequence;
    }

    private String resolveCallsite() {
        return STACK_WALKER.walk(stream -> stream
                .filter(frame -> !frame.getClassName().startsWith("engine."))
                .findFirst()
                .map(frame -> frame.getClassName() + ":" + frame.getMethodName() + ":" + frame.getLineNumber())
                .orElse("unknown"));
    }

    private String autoStepId(String callsite) {
        return "auto-" + Integer.toHexString(callsite.hashCode());
    }

    private static String normalizeStepId(String stepId) {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("Step id must not be blank");
        }
        return stepId.trim();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting on running step lease", interrupted);
        }
    }
}
