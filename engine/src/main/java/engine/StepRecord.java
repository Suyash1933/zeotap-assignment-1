package engine;

public record StepRecord(
        String workflowId,
        String stepKey,
        String stepId,
        StepStatus status,
        String outputJson,
        String outputType,
        String errorMessage,
        int attempt,
        String owner,
        long startedAtMs,
        long updatedAtMs) {
}
