package com.github.lisama569.fleetmissionorchestratorjava.model;

import java.util.List;

/**
 * 一份带依赖关系的任务计划。
 * arrivalTick 为该计划进入系统的模拟时间，用于保证调度结论可复现。
 */
public record Plan(String planId,
                   long arrivalTick,
                   List<TaskSpec> tasks) {

    public Plan {
        if (planId == null || planId.isBlank()) {
            throw new IllegalArgumentException("planId must not be blank");
        }
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
    }

    public static Plan of(String planId, long arrivalTick, TaskSpec... tasks) {
        return new Plan(planId, arrivalTick, List.of(tasks));
    }

    public TaskSpec task(String taskId) {
        return tasks.stream()
                .filter(t -> t.taskId().equals(taskId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no such task: " + taskId));
    }

    /**
     * 返回在末尾追加了一批新任务的新计划（本记录不可变，原计划不受影响）。
     * 追加前后的任务顺序保持稳定，保证调度结论可复现。
     */
    public Plan withAppendedTasks(java.util.List<TaskSpec> newTasks) {
        java.util.List<TaskSpec> combined = new java.util.ArrayList<>(tasks);
        combined.addAll(newTasks);
        return new Plan(planId, arrivalTick, combined);
    }
}
