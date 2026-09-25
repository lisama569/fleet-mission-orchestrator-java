package com.github.lisama569.fleetmissionorchestratorjava.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一次提交的任务计划（运行时实体）。
 *
 * <p>计划级状态由各步骤状态确定性地派生：
 * <ul>
 *   <li>任一步骤 FAILED / TIMED_OUT 终态 -> FAILED_FINAL（其余未完成步骤级联取消，已完成保留）</li>
 *   <li>外部取消 -> CANCELLED（已完成步骤保留）</li>
 *   <li>全部步骤 SUCCEEDED -> COMPLETED</li>
 *   <li>否则，只要有步骤已开始 -> RUNNING；连分配都还没发生 -> ACCEPTED</li>
 * </ul>
 */
public class Plan {

    private final String id;
    private final int priority;
    private final Map<String, Step> stepsById;
    private final List<Step> steps;

    private PlanState state = PlanState.ACCEPTED;

    public Plan(String id, int priority, List<Step> steps) {
        this.id = id;
        this.priority = priority;
        this.steps = List.copyOf(steps);
        Map<String, Step> map = new LinkedHashMap<>();
        for (Step step : steps) {
            map.put(step.getId(), step);
        }
        this.stepsById = Map.copyOf(map);
    }

    public String getId() { return id; }
    public int getPriority() { return priority; }
    public List<Step> getSteps() { return steps; }
    public Step getStep(String stepId) { return stepsById.get(stepId); }

    public PlanState getState() { return state; }

    public boolean isFinished() {
        return state == PlanState.COMPLETED
                || state == PlanState.FAILED_FINAL
                || state == PlanState.CANCELLED;
    }

    /** 根据步骤状态重新计算计划状态；在全局锁内调用。 */
    public PlanState refreshState() {
        if (state == PlanState.CANCELLED) {
            return state;
        }
        boolean anyStarted = false;
        boolean allSucceeded = true;
        for (Step step : steps) {
            StepState s = step.getState();
            if (s == StepState.FAILED || s == StepState.TIMED_OUT) {
                state = PlanState.FAILED_FINAL;
                return state;
            }
            if (s != StepState.SUCCEEDED) {
                allSucceeded = false;
            }
            if (s != StepState.PENDING) {
                anyStarted = true;
            }
        }
        if (allSucceeded) {
            state = PlanState.COMPLETED;
        } else if (anyStarted) {
            state = PlanState.RUNNING;
        } else {
            state = PlanState.ACCEPTED;
        }
        return state;
    }

    /** 外部取消：保留已完成步骤，其余收敛为 CANCELLED。 */
    public void cancel() {
        for (Step step : steps) {
            step.cancel();
        }
        state = PlanState.CANCELLED;
    }

    /** 某步骤失败终态后，把计划内其余未完成步骤级联取消（已完成保留）。 */
    public void cascadeCancelAfterFailure() {
        for (Step step : steps) {
            step.cancel();
        }
        state = PlanState.FAILED_FINAL;
    }

    @Override
    public String toString() {
        return "Plan{" + id + ", priority=" + priority + ", state=" + state + ", steps=" + steps.size() + '}';
    }
}
