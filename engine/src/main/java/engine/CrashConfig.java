package engine;

import java.util.Objects;

public record CrashConfig(String stepId, CrashPhase phase) {
    public static final CrashConfig NONE = new CrashConfig(null, CrashPhase.NONE);

    public CrashConfig {
        Objects.requireNonNull(phase, "phase");
    }

    public boolean shouldCrash(String currentStepId, CrashPhase currentPhase) {
        if (phase == CrashPhase.NONE || currentPhase != phase) {
            return false;
        }
        if (stepId == null || stepId.isBlank()) {
            return true;
        }
        return stepId.equals(currentStepId);
    }
}
