package com.github.lisama569.fleetmissionorchestratorjava.scheduler;

/**
 * 计划声明的固定时间窗保留（提交时做资源冲突检测）。
 */
public record Reservation(String robotId, long fromMillis, long toMillis, String planId, String stepId) {
}
