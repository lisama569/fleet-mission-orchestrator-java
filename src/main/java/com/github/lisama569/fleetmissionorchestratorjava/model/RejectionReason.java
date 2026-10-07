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
    RESOURCE_CONFLICT,
    /** 追加步骤时新批次为空。 */
    INVALID_APPEND,
    /** 追加批次内部出现重复步骤编号。 */
    DUPLICATE_TASK_ID_IN_APPEND,
    /** 新步骤编号与计划中已接受的步骤冲突（已接受步骤不可改动/占用）。 */
    TASK_ID_ALREADY_EXISTS,
    /** 目标计划已收敛为终态，不能再追加步骤。 */
    PLAN_NOT_APPENDABLE
}
