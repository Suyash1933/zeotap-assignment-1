package app;

import engine.CrashConfig;
import engine.CrashPhase;
import engine.DurableEngine;
import examples.onboarding.EmployeeOnboardingWorkflow;
import examples.onboarding.OnboardingRequest;
import examples.onboarding.OnboardingResult;
import examples.onboarding.OnboardingServices;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class App {
    private static final Pattern ID_PATTERN = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$");
    private static final Pattern STEP_PATTERN = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$");
    private static final Pattern NAME_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z .'-]{1,99}$");
    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,253}\\.[A-Za-z]{2,63}$");
    private static final Set<String> ALLOWED_OPTIONS = Set.of(
            "workflow-id", "employee-id", "name", "email", "db", "crash-step", "crash-phase", "help");
    private static final Set<String> ALLOWED_DB_EXTENSIONS = Set.of(".db", ".sqlite", ".sqlite3");

    private App() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> argMap = parseArgs(args);
        if (argMap.containsKey("help")) {
            printUsage();
            return;
        }

        String workflowId = validateValue(
                "workflow-id",
                argMap.getOrDefault("workflow-id", "wf-employee-001"),
                ID_PATTERN,
                64);
        String employeeId = validateValue(
                "employee-id",
                argMap.getOrDefault("employee-id", "emp-001"),
                ID_PATTERN,
                64);
        String fullName = validateValue("name", argMap.getOrDefault("name", "Ada Lovelace"), NAME_PATTERN, 100);
        String email = validateValue("email", argMap.getOrDefault("email", "ada@example.com"), EMAIL_PATTERN, 320);
        Path dbPath = resolveSafeDbPath(argMap.getOrDefault("db", "durable.db"));
        String crashStep = argMap.get("crash-step") == null
                ? null
                : validateValue("crash-step", argMap.get("crash-step"), STEP_PATTERN, 64);
        CrashPhase crashPhase = CrashPhase.fromValue(argMap.getOrDefault("crash-phase", "none"));

        CrashConfig crashConfig = new CrashConfig(crashStep, crashPhase);

        OnboardingRequest request = new OnboardingRequest(employeeId, fullName, email);

        System.out.println("Workflow ID     : " + mask(workflowId));
        System.out.println("Employee ID     : " + mask(employeeId));
        System.out.println("Employee Email  : " + maskEmail(email));
        System.out.println("Database        : " + dbPath.getFileName());
        System.out.println("Crash step      : " + (crashStep == null ? "<none>" : mask(crashStep)));
        System.out.println("Crash phase     : " + crashPhase);

        runWorkflow(workflowId, dbPath, crashConfig, request);
    }

    private static void runWorkflow(String workflowId,
                                    Path dbPath,
                                    CrashConfig crashConfig,
                                    OnboardingRequest request) throws Exception {
        try {
            OnboardingServices services = new OnboardingServices(dbPath);
            services.initialize();

            DurableEngine engine = DurableEngine.forSqlite(dbPath, crashConfig);
            try (EmployeeOnboardingWorkflow workflow = new EmployeeOnboardingWorkflow(services)) {
                OnboardingResult result = engine.run(workflowId, ctx -> workflow.run(ctx, request));
                System.out.println("Workflow finished successfully.");
                System.out.println("Employee        : " + mask(result.employeeId()));
                System.out.println("Laptop ID       : " + mask(result.laptopId()));
                System.out.println("Access ID       : " + mask(result.accessId()));
                System.out.println("Welcome Email ID: " + mask(result.welcomeEmailMessageId()));
            }
        } catch (SQLException sqlException) {
            String state = sqlException.getSQLState() == null ? "n/a" : sqlException.getSQLState();
            System.err.printf(
                    "Database error while running workflow (SQLState=%s, code=%d).%n",
                    state,
                    sqlException.getErrorCode());
            throw sqlException;
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--help".equals(arg) || "-h".equals(arg)) {
                parsed.put("help", "true");
                continue;
            }
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for argument: " + arg);
            }
            String key = arg.substring(2);
            if (!ALLOWED_OPTIONS.contains(key)) {
                throw new IllegalArgumentException("Unsupported argument: " + arg);
            }
            if (parsed.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate argument provided: " + arg);
            }

            String value = args[++i];
            if (value.length() > 512) {
                throw new IllegalArgumentException("Argument too long for " + arg);
            }
            if (containsControlChars(value)) {
                throw new IllegalArgumentException("Invalid control characters in " + arg);
            }
            parsed.put(key, value.trim());
        }
        return parsed;
    }

    private static String validateValue(String fieldName, String value, Pattern pattern, int maxLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        if (value.length() > maxLength) {
            throw new IllegalArgumentException(fieldName + " exceeds max length " + maxLength);
        }
        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid format for " + fieldName);
        }
        return value;
    }

    private static Path resolveSafeDbPath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("db path must not be blank");
        }
        if (rawPath.length() > 255) {
            throw new IllegalArgumentException("db path exceeds max length");
        }
        if (containsControlChars(rawPath) || rawPath.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("db path contains invalid characters");
        }

        Path baseDir = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path candidate;
        try {
            candidate = Path.of(rawPath);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid db path format", e);
        }

        Path resolved = candidate.isAbsolute()
                ? candidate.toAbsolutePath().normalize()
                : baseDir.resolve(candidate).normalize();

        String lowerName = resolved.getFileName().toString().toLowerCase();
        boolean extensionAllowed = ALLOWED_DB_EXTENSIONS.stream().anyMatch(lowerName::endsWith);
        if (!extensionAllowed) {
            throw new IllegalArgumentException("db file must use .db, .sqlite, or .sqlite3 extension");
        }

        try {
            Path realBase = baseDir.toRealPath();
            Path parent = resolved.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path realParent = parent == null ? realBase : parent.toRealPath();
            if (!realParent.startsWith(realBase)) {
                throw new IllegalArgumentException("db path traversal is not allowed");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to prepare db directory", e);
        }
        return resolved;
    }

    private static String maskEmail(String email) {
        int at = email.indexOf('@');
        if (at <= 1) {
            return "***";
        }
        String local = email.substring(0, at);
        String domain = email.substring(at + 1);
        return local.charAt(0) + "***@" + domain;
    }

    private static String mask(String value) {
        if (value == null || value.isEmpty()) {
            return "***";
        }
        if (value.length() <= 4) {
            return "***";
        }
        return value.substring(0, 2) + "***" + value.substring(value.length() - 2);
    }

    private static boolean containsControlChars(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  mvn -q -pl app exec:java -Dexec.args=\"[options]\"");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --workflow-id <id>      Workflow instance id (resume by reusing this id)");
        System.out.println("  --employee-id <id>      Employee id");
        System.out.println("  --name <full name>      Employee full name");
        System.out.println("  --email <email>         Employee email");
        System.out.println("  --db <sqlite.db>        SQLite path (default: durable.db)");
        System.out.println("  --crash-step <step-id>  Optional step id to crash at");
        System.out.println("  --crash-phase <phase>   none | before-execute | after-execute-before-commit | after-commit");
    }
}
