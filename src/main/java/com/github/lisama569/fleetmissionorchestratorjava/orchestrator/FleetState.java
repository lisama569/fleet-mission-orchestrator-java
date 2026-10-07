package com.github.lisama569.fleetmissionorchestratorjava.orchestrator;

import com.github.lisama569.fleetmissionorchestratorjava.model.DecisionEntry;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntime;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntimeView;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineKind;

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
    private final List<TimelineEvent> timeline = new ArrayList<>();
    private long timelineSeq;
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

    /**
     * 用合并后的计划整体替换已接受计划：已有步骤的运行时原样保留（已完成工作不丢），
     * 仅为新追加的步骤创建运行时。
     */
    public void replacePlan(Plan merged, long atTick) {
        String planId = merged.planId();
        Map<String, TaskRuntime> rts = runtimesByPlan.get(planId);
        if (rts == null) {
            throw new IllegalArgumentException("plan not registered: " + planId);
        }
        for (TaskSpec t : merged.tasks()) {
            rts.putIfAbsent(t.taskId(), new TaskRuntime(planId, t));
        }
        plans.put(planId, merged);
        lastChangeTick.put(planId, atTick);
    }

    /** 为计划追加运行时（一般由 replacePlan 内部完成）。 */
    public void registerTaskRuntimes(String planId, List<TaskSpec> added) {
        Map<String, TaskRuntime> rts = runtimesByPlan.get(planId);
        for (TaskSpec t : added) {
            rts.putIfAbsent(t.taskId(), new TaskRuntime(planId, t));
        }
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

    /**
     * 记录一条全局时间线事件。同一 tick 内的最终顺序由
     * {@link #timeline()} 的确定性排序保证，因此事件的真实到达线程不影响复盘结果。
     */
    public void timeline(String planId, long tick, String taskId, String robotId,
                         TimelineKind kind, int attempt, String message) {
        timeline.add(new TimelineEvent(++timelineSeq, tick, planId,
                taskId == null ? "" : taskId,
                robotId == null ? "" : robotId,
                kind, attempt, message));
    }

    /** 全量时间线，按 (tick, 种类顺序, planId, taskId, seq) 确定性排序。 */
    public List<TimelineEvent> timeline() {
        List<TimelineEvent> copy = new ArrayList<>(timeline);
        copy.sort(Comparator.comparingLong(TimelineEvent::tick)
                .thenComparingInt(e -> e.kind().order())
                .thenComparing(TimelineEvent::planId)
                .thenComparing(TimelineEvent::taskId)
                .thenComparingLong(TimelineEvent::seq));
        return List.copyOf(copy);
    }

    /** 单个计划的时间线，排序规则与全量时间线一致。 */
    public List<TimelineEvent> timeline(String planId) {
        return timeline().stream().filter(e -> e.planId().equals(planId)).toList();
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
