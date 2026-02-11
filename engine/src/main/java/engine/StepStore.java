package engine;

import java.sql.SQLException;

public interface StepStore {
    StepReservation reserve(String workflowId, String stepKey, String stepId, String owner, long leaseMs) throws SQLException;

    void complete(String workflowId, String stepKey, String owner, String outputJson, String outputType) throws SQLException;

    void fail(String workflowId, String stepKey, String owner, String errorMessage) throws SQLException;

    void initialize() throws SQLException;
}
