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
     * 运行中追加步骤的校验。原有计划此前已通过校验，这里只针对“新批次 + 合并后的整体”检查：
     * 1) 新批次非空；
     * 2) 新批次内部步骤编号不重复（DUPLICATE_TASK_ID_IN_APPEND）；
     * 3) 新步骤编号不与已接受步骤冲突（TASK_ID_ALREADY_EXISTS；已接受步骤不可被改动/顶替）；
     * 4) 新步骤的依赖在“合并后的计划”中都存在（MISSING_DEPENDENCY）——可依赖已有步骤或同批步骤；
     * 5) 合并后的依赖图无环（CYCLIC_DEPENDENCY）；
     * 6) 新步骤绑定的机器人必须存在（UNKNOWN_REQUIRED_ROBOT）。
     * 只有全部通过才返回合并后的计划，由调用方继续做整体可行性排程；任何一项失败都整体拒绝。
     */
    public Plan validateAppend(Plan existing, List<TaskSpec> appended, Map<String, Robot> robots) {
        if (appended == null || appended.isEmpty()) {
            throw new PlanRejectedException(RejectionReason.INVALID_APPEND,
                    "append batch for plan " + existing.planId() + " must contain at least one task");
        }

        // 2) 批次内部编号唯一
        Set<String> newIds = new HashSet<>();
        for (TaskSpec t : appended) {
            if (!newIds.add(t.taskId())) {
                throw new PlanRejectedException(RejectionReason.DUPLICATE_TASK_ID_IN_APPEND,
                        "duplicated task id within append batch: " + t.taskId());
            }
        }

        // 3) 不得与已接受步骤同号
        for (TaskSpec t : appended) {
            boolean clash = existing.tasks().stream()
                    .anyMatch(x -> x.taskId().equals(t.taskId()));
            if (clash) {
                throw new PlanRejectedException(RejectionReason.TASK_ID_ALREADY_EXISTS,
                        "task " + t.taskId() + " already accepted in plan "
                                + existing.planId() + "; accepted tasks cannot be modified");
            }
        }

        // 先构造合并计划（只读检查，尚未写回状态，故失败时原计划完全不受影响）
        Plan merged = existing.withAppendedTasks(appended);

        // 4) 新步骤的依赖在合并图内必须存在
        Set<String> mergedIds = new HashSet<>();
        for (TaskSpec t : merged.tasks()) {
            mergedIds.add(t.taskId());
        }
        for (TaskSpec t : appended) {
            for (String dep : t.dependencies()) {
                if (!mergedIds.contains(dep)) {
                    throw new PlanRejectedException(RejectionReason.MISSING_DEPENDENCY,
                            "appended task " + t.taskId() + " depends on missing task " + dep);
                }
            }
        }

        // 5) 合并后的整体依赖图无环（可发现“新步骤与已接受步骤之间”的环）
        topoOrder(merged);

        // 6) 新步骤绑定机器人必须存在
        for (TaskSpec t : appended) {
            String rid = t.requiredRobotId();
            if (rid != null && !robots.containsKey(rid)) {
                throw new PlanRejectedException(RejectionReason.UNKNOWN_REQUIRED_ROBOT,
                        "appended task " + t.taskId() + " requires unknown robot " + rid);
            }
        }
        return merged;
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
