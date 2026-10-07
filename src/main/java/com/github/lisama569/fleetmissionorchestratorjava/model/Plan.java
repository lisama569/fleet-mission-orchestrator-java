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
     * 返回一份在本计划末尾追加新步骤后的新计划：planId 与 arrivalTick 保持不变，
     * 原有步骤（规格、顺序）原样保留。用于“运行中追加步骤”被接受后的整体替换。
     */
    public Plan withAppendedTasks(List<TaskSpec> appended) {
        List<TaskSpec> merged = new java.util.ArrayList<>(tasks);
        merged.addAll(appended);
        return new Plan(planId, arrivalTick, List.copyOf(merged));
    }
}
