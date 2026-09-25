package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 计划被拒绝时可区分的失败原因。
 */
public enum RejectionReason {
    INVALID_PLAN,
    DUPLICATE_PLAN_ID,
    DUPLICATE_TASK_ID,
    MISSING_DEPENDENCY,
    CYCLIC_DEPENDENCY,
    UNKNOWN_REQUIRED_ROBOT,
    NO_CAPABLE_ROBOT,
    RESOURCE_CONFLICT
}
