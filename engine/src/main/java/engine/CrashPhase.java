package engine;

public enum CrashPhase {
    NONE,
    BEFORE_EXECUTE,
    AFTER_EXECUTE_BEFORE_COMMIT,
    AFTER_COMMIT;

    public static CrashPhase fromValue(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        return switch (value.trim().toLowerCase()) {
            case "before-execute" -> BEFORE_EXECUTE;
            case "after-execute-before-commit" -> AFTER_EXECUTE_BEFORE_COMMIT;
            case "after-commit" -> AFTER_COMMIT;
            case "none" -> NONE;
            default -> throw new IllegalArgumentException("Unsupported crash phase: " + value);
        };
    }
}
