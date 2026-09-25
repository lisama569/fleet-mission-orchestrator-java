package com.github.lisama569.fleetmissionorchestratorjava.domain;

/**
 * 单步状态机：
 * PENDING -> ASSIGNED -> RUNNING -> SUCCEEDED
 *                  \------> FAILED ->（重试）PENDING
 * RUNNING 可因超时进入 TIMED_OUT，随后按剩余重试次数回到 PENDING
 * 计划取消时未完成步骤收敛到 CANCELLED
 */
public enum StepState {
    PENDING,
    ASSIGNED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED
}
