package com.github.lisama569.fleetmissionorchestratorjava.domain;

/**
 * 计划整体状态：
 * ACCEPTED -> RUNNING -> COMPLETED
 *           \-> FAILED_FINAL（某步骤重试用尽）
 * 任意未完成阶段 -> CANCELLED（取消时已完成步骤保留）
 */
public enum PlanState {
    ACCEPTED,
    RUNNING,
    COMPLETED,
    FAILED_FINAL,
    CANCELLED
}
