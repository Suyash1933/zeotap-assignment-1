package engine;

public record StepReservation(ReservationState state, StepRecord record) {
    public static StepReservation acquired(StepRecord record) {
        return new StepReservation(ReservationState.ACQUIRED, record);
    }

    public static StepReservation cached(StepRecord record) {
        return new StepReservation(ReservationState.CACHED, record);
    }

    public static StepReservation runningElsewhere(StepRecord record) {
        return new StepReservation(ReservationState.RUNNING_ELSEWHERE, record);
    }
}
