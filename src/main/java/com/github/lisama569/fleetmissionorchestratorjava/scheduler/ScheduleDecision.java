package com.github.lisama569.fleetmissionorchestratorjava.scheduler;

/**
 * 一次调度决策的可解释记录（用于日志与复现）。
 */
public record ScheduleDecision(long clockMillis, String action, String planId, String stepId, String robotId, String detail) {
}
