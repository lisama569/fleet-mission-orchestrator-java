package com.github.lisama569.fleetmissionorchestratorjava.model;

import java.util.List;
import java.util.Map;

/**
 * 一份计划的确定性调度结果：任务 -> 分配列表。
 * feasible=false 时 assignments 为空、reason 给出可区分原因。
 */
public record Schedule(String planId,
                       boolean feasible,
                       RejectionReason reason,
                       List<TaskAssignment> assignments,
                       Map<String, Long> finishTickByTask) {

    public static Schedule rejected(String planId, RejectionReason reason) {
        return new Schedule(planId, false, reason, List.of(), Map.of());
    }

    public static Schedule accepted(String planId,
                                    List<TaskAssignment> assignments,
                                    Map<String, Long> finishTickByTask) {
        return new Schedule(planId, true, null, List.copyOf(assignments), Map.copyOf(finishTickByTask));
    }
}
