package com.github.lisama569.fleetmissionorchestratorjava.domain;

/**
 * 计划提交 / 重规划被拒绝时的可区分原因。
 */
public enum RejectionReason {
    /** 计划内依赖引用了不存在的步骤。 */
    UNKNOWN_DEPENDENCY,
    /** 步骤之间存在循环依赖。 */
    CYCLIC_DEPENDENCY,
    /** 计划声明的固定占用时间窗与既有保留声明重叠（同一资源）。 */
    RESOURCE_CONFLICT,
    /** 请求的机器人不存在，或没有任何机器人具备要求的能力。 */
    NO_MATCHING_ROBOT,
    /** 请求体非法（空计划、重复步骤 id 等）。 */
    INVALID_PLAN
}
