package com.github.lisama569.fleetmissionorchestratorjava.scheduler;

import com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 静态校验：任务 id 唯一、依赖存在、无循环依赖、绑定机器人存在。
 * 校验失败抛出带 RejectionReason 的 PlanRejectedException，原因可区分。
 */
public final class PlanValidator {

    public void validate(Plan plan, Map<String, Robot> robots) {
        List<TaskSpec> tasks = plan.tasks();
        if (tasks.isEmpty()) {
            throw new PlanRejectedException(RejectionReason.INVALID_PLAN,
                    "plan " + plan.planId() + " has no tasks");
        }

        // 1) 任务 id 唯一
        Set<String> ids = new HashSet<>();
        for (TaskSpec t : tasks) {
            if (!ids.add(t.taskId())) {
                throw new PlanRejectedException(RejectionReason.DUPLICATE_TASK_ID,
                        "duplicated task id within plan: " + t.taskId());
            }
        }

        // 2) 依赖必须存在
        for (TaskSpec t : tasks) {
            for (String dep : t.dependencies()) {
                if (!ids.contains(dep)) {
                    throw new PlanRejectedException(RejectionReason.MISSING_DEPENDENCY,
                            "task " + t.taskId() + " depends on missing task " + dep);
                }
            }
        }

        // 3) 无循环依赖（同时完成一次拓扑排序探测）
        topoOrder(plan);

        // 4) 绑定机器人必须存在
        for (TaskSpec t : tasks) {
            String rid = t.requiredRobotId();
            if (rid != null && !robots.containsKey(rid)) {
                throw new PlanRejectedException(RejectionReason.UNKNOWN_REQUIRED_ROBOT,
                        "task " + t.taskId() + " requires unknown robot " + rid);
            }
        }
    }

    /**
     * Kahn 拓扑排序，按任务 id 字典序打破平局（保证确定性）。
     * 存在环时抛 CYCLIC_DEPENDENCY 并列出环节点。
     */
    public List<String> topoOrder(Plan plan) {
        Map<String, TaskSpec> byId = new HashMap<>();
        Map<String, Integer> indegree = new HashMap<>();
        for (TaskSpec t : plan.tasks()) {
            byId.put(t.taskId(), t);
            indegree.put(t.taskId(), 0);
        }
        for (TaskSpec t : plan.tasks()) {
            for (String dep : t.dependencies()) {
                indegree.merge(t.taskId(), 1, Integer::sum);
            }
        }

        java.util.PriorityQueue<String> queue = new java.util.PriorityQueue<>();
        indegree.entrySet().stream()
                .filter(e -> e.getValue() == 0)
                .map(Map.Entry::getKey)
                .forEach(queue::add);

        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(id);
            // 找到以 id 为依赖的后继
            for (TaskSpec t : plan.tasks()) {
                if (t.dependencies().contains(id)) {
                    int d = indegree.merge(t.taskId(), -1, Integer::sum);
                    if (d == 0) {
                        queue.add(t.taskId());
                    }
                }
            }
        }

        if (order.size() != plan.tasks().size()) {
            Set<String> inCycle = new HashSet<>(byId.keySet());
            inCycle.removeAll(order);
            throw new PlanRejectedException(RejectionReason.CYCLIC_DEPENDENCY,
                    "cyclic dependency detected among tasks: " + inCycle.stream().sorted().toList());
        }
        return order;
    }
}
