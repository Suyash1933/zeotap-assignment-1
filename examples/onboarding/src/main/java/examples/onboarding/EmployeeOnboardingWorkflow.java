package examples.onboarding;

import engine.DurableContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class EmployeeOnboardingWorkflow implements AutoCloseable {
    private final OnboardingServices services;
    private final Executor executor;

    public EmployeeOnboardingWorkflow(OnboardingServices services) {
        this(services, Executors.newFixedThreadPool(4));
    }

    public EmployeeOnboardingWorkflow(OnboardingServices services, Executor executor) {
        this.services = services;
        this.executor = executor;
    }

    public OnboardingResult run(DurableContext context, OnboardingRequest request) throws Exception {
        context.step("create-record", () -> services.createEmployeeRecord(request));

        CompletableFuture<ProvisionResponse> laptopFuture =
                context.stepAsync("provision-laptop", () -> services.provisionLaptop(request.employeeId()), executor);
        CompletableFuture<ProvisionResponse> accessFuture =
                context.stepAsync("provision-access", () -> services.provisionAccess(request.employeeId()), executor);

        ProvisionResponse laptop = await(laptopFuture);
        ProvisionResponse access = await(accessFuture);

        EmailReceipt emailReceipt = context.step(
                "send-welcome-email",
                () -> services.sendWelcomeEmail(request.employeeId(), request.email()));

        return new OnboardingResult(
                request.employeeId(),
                laptop.resourceId(),
                access.resourceId(),
                emailReceipt.messageId());
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof Exception checked) {
                throw checked;
            }
            throw e;
        }
    }

    @Override
    public void close() {
        if (executor instanceof java.util.concurrent.ExecutorService service) {
            service.shutdown();
        }
    }
}
