package com.github.lisama569.fleetmissionorchestratorjava.exception;

import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;

/**
 * 计划提交时被拒绝，携带可区分的失败原因与可读说明。
 */
public class PlanRejectedException extends RuntimeException {
    private final RejectionReason reason;

    public PlanRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
