package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * TaskRuntime 的不可变快照（供 REST 与测试读取，避免外部修改内部状态）。
 */
public record TaskRuntimeView(String taskId,
                              String planId,
                              TaskStatus status,
                              int attempts,
                              long startTick,
                              long endTick,
                              String assignedRobotId,
                              AttemptOutcome lastOutcome,
                              String taskType,
                              int priority,
                              long durationTicks) {
}
