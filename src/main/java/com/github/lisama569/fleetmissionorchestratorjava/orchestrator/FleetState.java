package com.github.lisama569.fleetmissionorchestratorjava.orchestrator;

import com.github.lisama569.fleetmissionorchestratorjava.model.DecisionEntry;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntime;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntimeView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 全部内存状态：机器人池、已接受计划、任务运行时、计划状态、决策日志与模拟时钟。
 * 本类自身不加锁，所有访问由 {@link FleetOrchestrator} 在同一把锁内串行化。
 */
public final class FleetState {

    private final Map<String, Robot> robots = new TreeMap<>();
    private final Map<String, Plan> plans = new TreeMap<>();
    private final Map<String, PlanStatus> planStatus = new TreeMap<>();
    private final Map<String, Map<String, TaskRuntime>> runtimesByPlan = new TreeMap<>();
    private final Map<String, List<DecisionEntry>> decisionsByPlan = new TreeMap<>();
    private final Map<String, Long> acceptedTick = new TreeMap<>();
    private final Map<String, Long> lastChangeTick = new TreeMap<>();
    private long tick;

    public FleetState(List<Robot> robots) {
        for (Robot r : robots) {
            this.robots.put(r.id(), r);
        }
    }

    public Map<String, Robot> robots() {
        return robots;
    }

    public Map<String, Plan> plans() {
        return plans;
    }

    public long tick() {
        return tick;
    }

    public void setTick(long tick) {
        this.tick = tick;
    }

    public PlanStatus planStatus(String planId) {
        return planStatus.get(planId);
    }

    public Map<String, PlanStatus> planStatuses() {
        return planStatus;
    }

    /** 注册一份已接受计划，初始化所有任务的运行时。 */
    public void registerPlan(Plan plan, long atTick) {
        plans.put(plan.planId(), plan);
        planStatus.put(plan.planId(), PlanStatus.ACCEPTED);
        acceptedTick.put(plan.planId(), atTick);
        lastChangeTick.put(plan.planId(), atTick);
        Map<String, TaskRuntime> rts = new LinkedHashMap<>();
        for (var t : plan.tasks()) {
            rts.put(t.taskId(), new TaskRuntime(plan.planId(), t));
        }
        runtimesByPlan.put(plan.planId(), rts);
        decisionsByPlan.put(plan.planId(), new ArrayList<>());
    }

    public Map<String, TaskRuntime> taskRuntimes(String planId) {
        return runtimesByPlan.getOrDefault(planId, Map.of());
    }

    /** 跨计划的扁平运行时表，key = planId::taskId。 */
    public Map<String, TaskRuntime> flatRuntimes() {
        Map<String, TaskRuntime> flat = new LinkedHashMap<>();
        for (var e : runtimesByPlan.entrySet()) {
            for (var rt : e.getValue().entrySet()) {
                flat.put(e.getKey() + "::" + rt.getKey(), rt.getValue());
            }
        }
        return flat;
    }

    public void setPlanStatus(String planId, PlanStatus status, long tick) {
        planStatus.put(planId, status);
        lastChangeTick.put(planId, tick);
    }

    public void log(String planId, long tick, String category, String message) {
        decisionsByPlan.computeIfAbsent(planId, k -> new ArrayList<>())
                .add(new DecisionEntry(tick, category, message));
    }

    public List<DecisionEntry> decisions(String planId) {
        return List.copyOf(decisionsByPlan.getOrDefault(planId, List.of()));
    }

    public PlanSnapshot snapshot(String planId) {
        Plan plan = plans.get(planId);
        if (plan == null) {
            throw new IllegalArgumentException("unknown plan: " + planId);
        }
        List<TaskRuntimeView> views = new ArrayList<>();
        for (var rt : taskRuntimes(planId).values()) {
            var spec = rt.spec();
            views.add(new TaskRuntimeView(spec.taskId(), planId, rt.status(), rt.attempts(),
                    rt.startTick(), rt.endTick(), rt.assignedRobotId(), rt.lastOutcome(),
                    spec.type(), spec.priority(), spec.durationTicks()));
        }
        views.sort(Comparator.comparing(TaskRuntimeView::taskId));
        return new PlanSnapshot(planId, planStatus.get(planId), List.copyOf(views),
                decisions(planId), acceptedTick.getOrDefault(planId, 0L),
                lastChangeTick.getOrDefault(planId, 0L));
    }
}
