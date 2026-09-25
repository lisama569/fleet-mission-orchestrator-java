package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 调度器产出的单次任务分配决策。
 * taskId/robotId       分配关系
 * startTick/endTick    模拟时间区间（左闭右开）
 * attempt              这是该任务的第几次尝试（1 起）
 */
public record TaskAssignment(String planId,
                             String taskId,
                             String robotId,
                             long startTick,
                             long endTick,
                             int attempt) {
}
