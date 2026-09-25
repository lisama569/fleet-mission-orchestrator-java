package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 任务（步骤）状态。
 * PENDING   -> 已接收/等待重试，尚未运行
 * RUNNING   -> 已占用机器人正在执行
 * SUCCEEDED -> 成功（终态，不会因重试/重规划重复生效）
 * FAILED    -> 重试次数耗尽后的失败终态
 * TIMED_OUT -> 超时且重试次数耗尽的终态
 * CANCELLED -> 被取消（终态）
 * SKIPPED   -> 依赖失败或被取消，永远无法执行（终态）
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED,
    SKIPPED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == TIMED_OUT
                || this == CANCELLED || this == SKIPPED;
    }
}
