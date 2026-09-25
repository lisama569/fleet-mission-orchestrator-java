package com.github.lisama569.fleetmissionorchestratorjava.model;

import java.util.List;

/**
 * 一份计划的完整状态快照。
 */
public record PlanSnapshot(String planId,
                           PlanStatus status,
                           List<TaskRuntimeView> tasks,
                           List<DecisionEntry> decisions,
                           long acceptedTick,
                           long lastChangeTick) {

    public List<TaskRuntimeView> tasksByStatus(TaskStatus s) {
        return tasks.stream().filter(t -> t.status() == s).toList();
    }
}
