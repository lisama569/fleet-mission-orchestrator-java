package com.github.lisama569.fleetmissionorchestratorjava.domain;

/**
 * 计划校验失败异常，携带可区分的 {@link RejectionReason}。
 */
public class PlanValidationException extends RuntimeException {

    private final RejectionReason reason;

    public PlanValidationException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason getReason() {
        return reason;
    }
}
