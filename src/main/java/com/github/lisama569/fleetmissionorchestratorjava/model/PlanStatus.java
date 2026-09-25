package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 计划状态：ACCEPTED(已通过校验与可行性调度) -> EXECUTING -> COMPLETED/FAILED/CANCELLED。
 */
public enum PlanStatus {
    ACCEPTED,
    EXECUTING,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
