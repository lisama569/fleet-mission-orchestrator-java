package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 内存模拟机器人执行任务时的行为模型（不依赖任何外部服务）：
 * SUCCEED             首次尝试即成功
 * FAIL_THEN_SUCCEED   前 flakyAttempts 次失败，之后成功
 * TIMEOUT_THEN_SUCCEED 前 flakyAttempts 次超时，之后成功
 * FAIL_ALWAYS         每次都失败（用于验证重试耗尽 -> FAILED）
 */
public enum TaskBehavior {
    SUCCEED,
    FAIL_THEN_SUCCEED,
    TIMEOUT_THEN_SUCCEED,
    FAIL_ALWAYS;

    /**
     * @param attemptNumber 第几次尝试，从 1 开始
     * @param flakyAttempts 前多少次尝试不稳定
     */
    public AttemptOutcome outcomeFor(int attemptNumber, int flakyAttempts) {
        return switch (this) {
            case SUCCEED -> AttemptOutcome.SUCCESS;
            case FAIL_THEN_SUCCEED ->
                    attemptNumber <= flakyAttempts ? AttemptOutcome.FAILURE : AttemptOutcome.SUCCESS;
            case TIMEOUT_THEN_SUCCEED ->
                    attemptNumber <= flakyAttempts ? AttemptOutcome.TIMEOUT : AttemptOutcome.SUCCESS;
            case FAIL_ALWAYS -> AttemptOutcome.FAILURE;
        };
    }
}
