package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 单次执行尝试的结果。
 */
public enum AttemptOutcome {
    SUCCESS,
    FAILURE,
    TIMEOUT,
    PREEMPTED
}
