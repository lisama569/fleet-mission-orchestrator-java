package com.github.lisama569.fleetmissionorchestratorjava.scheduler;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.api.StepSpec;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanState;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanValidationException;
import com.github.lisama569.fleetmissionorchestratorjava.domain.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Step;
import com.github.lisama569.fleetmissionorchestratorjava.domain.StepState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调度器：整个编排系统唯一的状态变更点。
 *
 * <p>并发安全：所有公共方法都在同一把 {@link ReentrantLock} 下执行，状态读取也加锁，
 * 因此并发提交 / 取消 / 禁用机器人与 tick 结算之间不存在竞态；相同输入序列必然产生
 * 相同的调度决策序列（决策排序只依赖状态，不依赖线程调度与墙钟）。
 *
 * <p>资源占用：RUNNING 步骤独占其机器人（occupiedBy）；同一机器人同一时刻绝不会被
 * 两个步骤占用。提交时声明的固定时间窗（pinned window）额外做跨计划的保留冲突检测。
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    /** 就绪步骤的确定性排序：优先级降序、计划 id 升序、步骤 id 升序。 */
    private static final Comparator<ReadyStep> READY_ORDER = Comparator
            .comparingInt((ReadyStep r) -> -r.plan.getPriority())
            .thenComparing(r -> r.plan.getId())
            .thenComparing(r -> r.step.getId());

    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<String, Robot> robots = new TreeMap<>();
    private final Map<String, Plan> plans = new TreeMap<>();
    private final List<Reservation> reservations = new ArrayList<>();
    private final List<ScheduleDecision> decisions = new ArrayList<>();

    /** robotId -> 运行键（planId + ":" + stepId）；资源独占表。 */
    private final Map<String, String> occupiedBy = new LinkedHashMap<>();
    /** 运行键 -> 运行中的任务结算信息。 */
    private final Map<String, RunningTask> running = new LinkedHashMap<>();

    public Scheduler() {
        this(Clock.systemUTC());
    }

    public Scheduler(Clock clock) {
        this.clock = clock;
    }

    private record ReadyStep(Plan plan, Step step) {
        String key() {
            return plan.getId() + ":" + step.getId();
        }
    }

    private record RunningTask(String planId, String stepId, String robotId,
                               long startMillis, long dueMillis, long timeoutAtMillis) {
        String key() {
            return planId + ":" + stepId;
        }
    }

    // ---------------------------------------------------------------------
    // 资源注册
    // ---------------------------------------------------------------------

    public void registerRobot(Robot robot) {
        lock.lock();
        try {
            robots.put(robot.getId(), robot);
            record("REGISTER_ROBOT", null, null, robot.getId(),
                    "capabilities=" + robot.getCapabilities());
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------------
    // 计划提交：校验 -> 冲突检测 -> 注册
    // ---------------------------------------------------------------------

    public Plan submit(PlanSubmission submission) {
        lock.lock();
        try {
            Plan plan = validateAndBuild(submission);
            plans.put(plan.getId(), plan);
            for (Step step : plan.getSteps()) {
                if (step.isPinned()) {
                    reservations.add(new Reservation(
                            step.getRequiredRobotId(),
                            step.getPinnedFromMillis(), step.getPinnedToMillis(),
                            plan.getId(), step.getId()));
                }
            }
            record("ACCEPT_PLAN", plan.getId(), null, null,
                    "priority=" + plan.getPriority() + ", steps=" + plan.getSteps().size());
            log.info("[submit] plan={} priority={} steps={}",
                    plan.getId(), plan.getPriority(),
                    plan.getSteps().stream().map(Step::getId).toList());
            return plan;
        } finally {
            lock.unlock();
        }
    }

    private Plan validateAndBuild(PlanSubmission submission) {
        if (submission == null
                || submission.getId() == null || submission.getId().isBlank()
                || submission.getSteps() == null || submission.getSteps().isEmpty()) {
            throw reject(RejectionReason.INVALID_PLAN,
                    "plan id 不能为空且至少包含一个步骤");
        }
        if (plans.containsKey(submission.getId())) {
            throw reject(RejectionReason.INVALID_PLAN,
                    "plan id 已存在: " + submission.getId());
        }

        List<StepSpec> specs = submission.getSteps();
        Set<String> ids = new HashSet<>();
        for (StepSpec spec : specs) {
            if (spec.getId() == null || spec.getId().isBlank()) {
                throw reject(RejectionReason.INVALID_PLAN, "存在没有 id 的步骤");
            }
            if (!ids.add(spec.getId())) {
                throw reject(RejectionReason.INVALID_PLAN, "重复的步骤 id: " + spec.getId());
            }
            if (spec.getDurationMillis() <= 0) {
                throw reject(RejectionReason.INVALID_PLAN,
                        "步骤 " + spec.getId() + " durationMillis 必须为正");
            }
            boolean hasWindow = spec.getPinnedFromMillis() != null || spec.getPinnedToMillis() != null;
            if (hasWindow && (spec.getPinnedFromMillis() == null || spec.getPinnedToMillis() == null
                    || spec.getPinnedFromMillis() >= spec.getPinnedToMillis()
                    || spec.getRequiredRobotId() == null || spec.getRequiredRobotId().isBlank())) {
                throw reject(RejectionReason.INVALID_PLAN,
                        "步骤 " + spec.getId() + " 的固定时间窗必须同时给出 from<to 且指定 requiredRobotId");
            }
        }

        // 依赖引用必须存在
        for (StepSpec spec : specs) {
            for (String dep : spec.getDependencies()) {
                if (!ids.contains(dep)) {
                    throw reject(RejectionReason.UNKNOWN_DEPENDENCY,
                            "步骤 " + spec.getId() + " 依赖了不存在的步骤: " + dep);
                }
            }
        }

        // 循环依赖检测：DFS 三色标记
        Map<String, StepSpec> specMap = new LinkedHashMap<>();
        for (StepSpec spec : specs) {
            specMap.put(spec.getId(), spec);
        }
        Set<String> visiting = new HashSet<>();
        Set<String> settled = new HashSet<>();
        for (StepSpec spec : specs) {
            detectCycle(spec, specMap, visiting, settled);
        }

        // 机器人可匹配性 + 固定时间窗资源冲突
        for (StepSpec spec : specs) {
            List<Robot> candidates = matchingRobots(spec.getRequiredRobotId(), spec.getRequiredCapability());
            if (candidates.isEmpty()) {
                throw reject(RejectionReason.NO_MATCHING_ROBOT,
                        "步骤 " + spec.getId() + " 找不到匹配机器人"
                                + "（robotId=" + spec.getRequiredRobotId()
                                + ", capability=" + spec.getRequiredCapability() + "）");
            }
            if (spec.getPinnedFromMillis() != null) {
                String robotId = spec.getRequiredRobotId();
                for (Reservation existing : reservations) {
                    if (existing.robotId().equals(robotId)
                            && spec.getPinnedFromMillis() < existing.toMillis()
                            && existing.fromMillis() < spec.getPinnedToMillis()) {
                        throw reject(RejectionReason.RESOURCE_CONFLICT,
                                "步骤 " + spec.getId() + " 在机器人 " + robotId + " 上的固定窗口 ["
                                        + spec.getPinnedFromMillis() + "," + spec.getPinnedToMillis()
                                        + ") 与计划 " + existing.planId() + " 的步骤 " + existing.stepId()
                                        + " 的保留窗口 [" + existing.fromMillis() + "," + existing.toMillis() + ") 冲突");
                    }
                }
                // 同一计划内两个固定窗口在同一机器人上重叠也拒绝
                for (StepSpec other : specs) {
                    if (other != spec && robotId.equals(other.getRequiredRobotId())
                            && other.getPinnedFromMillis() != null
                            && spec.getPinnedFromMillis() < other.getPinnedToMillis()
                            && other.getPinnedFromMillis() < spec.getPinnedToMillis()) {
                        throw reject(RejectionReason.RESOURCE_CONFLICT,
                                "同一计划内步骤 " + spec.getId() + " 与 " + other.getId()
                                        + " 在机器人 " + robotId + " 上的固定窗口冲突");
                    }
                }
            }
        }

        List<Step> steps = specs.stream()
                .map(s -> new Step(s.getId(), s.getRequiredCapability(), s.getRequiredRobotId(),
                        s.getDurationMillis(), s.getTimeoutMillis(), s.getMaxAttempts(),
                        s.isAlwaysFail(),
                        s.getPinnedFromMillis() == null ? -1L : s.getPinnedFromMillis(),
                        s.getPinnedToMillis() == null ? Long.MAX_VALUE : s.getPinnedToMillis(),
                        s.getDependencies()))
                .toList();
        return new Plan(submission.getId(), submission.getPriority(), steps);
    }

    private void detectCycle(StepSpec spec, Map<String, StepSpec> specMap,
                             Set<String> visiting, Set<String> settled) {
        if (settled.contains(spec.getId())) {
            return;
        }
        if (!visiting.add(spec.getId())) {
            throw reject(RejectionReason.CYCLIC_DEPENDENCY,
                    "检测到循环依赖，环节点: " + spec.getId());
        }
        for (String dep : spec.getDependencies()) {
            detectCycle(specMap.get(dep), specMap, visiting, settled);
        }
        visiting.remove(spec.getId());
        settled.add(spec.getId());
    }

    /**
     * 机器人匹配：指定 robotId 时机器人必须存在；要求 capability 时必须具备；
     * 两者都给则取交集；都不给则任意可用机型均可（提交期只校验存在性，可用性在分配期判断）。
     */
    private List<Robot> matchingRobots(String robotId, String capability) {
        List<Robot> result = new ArrayList<>();
        for (Robot robot : robots.values()) {
            if (robotId != null && !robotId.isBlank() && !robot.getId().equals(robotId)) {
                continue;
            }
            if (capability != null && !capability.isBlank() && !robot.getCapabilities().contains(capability)) {
                continue;
            }
            result.add(robot);
        }
        return result;
    }

    // ---------------------------------------------------------------------
    // 重规划操作
    // ---------------------------------------------------------------------

    public void disableRobot(String robotId) {
        lock.lock();
        try {
            Robot robot = robots.get(robotId);
            if (robot == null) {
                throw reject(RejectionReason.NO_MATCHING_ROBOT, "机器人不存在: " + robotId);
            }
            robot.setAvailable(false);
            // 该机器人上正在运行的步骤全部被抢占：不增加尝试次数，回到 PENDING 等待重新分配
            List<String> preempted = new ArrayList<>();
            for (Map.Entry<String, RunningTask> entry : new ArrayList<>(running.entrySet())) {
                RunningTask task = entry.getValue();
                if (task.robotId().equals(robotId)) {
                    Plan plan = plans.get(task.planId());
                    Step step = plan.getStep(task.stepId());
                    step.preemptToPending();
                    running.remove(entry.getKey());
                    occupiedBy.remove(robotId);
                    preempted.add(task.key());
                    plan.refreshState();
                }
            }
            record("DISABLE_ROBOT", null, null, robotId,
                    "preempted=" + preempted);
            log.info("[replanning] robot={} disabled, preempted steps={}", robotId, preempted);
        } finally {
            lock.unlock();
        }
    }

    /** 恢复机器人可用；等待中的步骤会在后续 tick 被重新分配。 */
    public void enableRobot(String robotId) {
        lock.lock();
        try {
            Robot robot = robots.get(robotId);
            if (robot == null) {
                throw reject(RejectionReason.NO_MATCHING_ROBOT, "机器人不存在: " + robotId);
            }
            robot.setAvailable(true);
            record("ENABLE_ROBOT", null, null, robotId, "等待中的步骤将在后续 tick 重新分配");
            log.info("[replanning] robot={} enabled", robotId);
        } finally {
            lock.unlock();
        }
    }

    public void cancelPlan(String planId) {
        lock.lock();
        try {
            Plan plan = plans.get(planId);
            if (plan == null) {
                throw reject(RejectionReason.INVALID_PLAN, "计划不存在: " + planId);
            }
            if (plan.isFinished()) {
                record("CANCEL_PLAN_IGNORED", planId, null, null,
                        "计划已处于终态 " + plan.getState());
                return;
            }
            releasePlanResources(plan);
            plan.cancel();
            record("CANCEL_PLAN", planId, null, null, "已完成步骤保留，其余步骤取消");
            log.info("[replanning] plan={} cancelled, completed steps preserved", planId);
        } finally {
            lock.unlock();
        }
    }

    private void releasePlanResources(Plan plan) {
        for (Step step : plan.getSteps()) {
            String key = plan.getId() + ":" + step.getId();
            RunningTask task = running.remove(key);
            if (task != null) {
                occupiedBy.remove(task.robotId());
            }
        }
    }

    // ---------------------------------------------------------------------
    // 执行 tick：分配 -> 启动 -> 结算（超时优先于成功/失败判定）
    // ---------------------------------------------------------------------

    public void tick() {
        lock.lock();
        try {
            long now = clock.millis();
            settleRunning(now);
            dispatch(now);
        } finally {
            lock.unlock();
        }
    }

    private void dispatch(long now) {
        // 收集就绪步骤：依赖全部 SUCCEEDED 且自身 PENDING
        List<ReadyStep> ready = new ArrayList<>();
        for (Plan plan : plans.values()) {
            if (plan.isFinished()) {
                continue;
            }
            for (Step step : plan.getSteps()) {
                if (step.getState() != StepState.PENDING) {
                    continue;
                }
                boolean depsDone = true;
                for (String dep : step.getDependencies()) {
                    if (plan.getStep(dep).getState() != StepState.SUCCEEDED) {
                        depsDone = false;
                        break;
                    }
                }
                if (depsDone) {
                    ready.add(new ReadyStep(plan, step));
                }
            }
        }
        ready.sort(READY_ORDER);

        // 贪心分配：确定性地选择编号最小的空闲且匹配的可用机器人，随后立即启动
        for (ReadyStep r : ready) {
            Step step = r.step();
            if (step.getState() != StepState.PENDING) {
                continue;
            }
            Robot chosen = null;
            for (Robot robot : robots.values()) {
                if (!robot.isAvailable() || occupiedBy.containsKey(robot.getId())) {
                    continue;
                }
                if (step.getRequiredRobotId() != null && !step.getRequiredRobotId().isBlank()
                        && !step.getRequiredRobotId().equals(robot.getId())) {
                    continue;
                }
                if (step.getRequiredCapability() != null && !step.getRequiredCapability().isBlank()
                        && !robot.getCapabilities().contains(step.getRequiredCapability())) {
                    continue;
                }
                chosen = robot;
                break;
            }
            if (chosen == null) {
                record("WAIT_RESOURCE", r.plan.getId(), step.getId(), null,
                        "暂无空闲匹配机器人");
                continue;
            }
            step.markAssigned(chosen.getId());
            step.markRunning();
            String key = r.key();
            occupiedBy.put(chosen.getId(), key);
            long timeoutAt = step.getTimeoutMillis() > 0
                    ? now + step.getTimeoutMillis() : 0L;
            running.put(key, new RunningTask(r.plan.getId(), step.getId(), chosen.getId(),
                    now, now + step.getDurationMillis(), timeoutAt));
            r.plan.refreshState();
            record("ASSIGN_START", r.plan.getId(), step.getId(), chosen.getId(),
                    "attempt=" + step.getAttempt()
                            + ", due=" + (now + step.getDurationMillis())
                            + ", timeoutAt=" + (timeoutAt == 0 ? "none" : String.valueOf(timeoutAt)));
            log.info("[dispatch] plan={} step={} -> robot={} attempt={} priority={}",
                    r.plan.getId(), step.getId(), chosen.getId(), step.getAttempt(),
                    r.plan.getPriority());
        }
    }

    private void settleRunning(long now) {
        // 按启动时刻、再按运行键排序，保证结算顺序确定
        List<RunningTask> tasks = new ArrayList<>(running.values());
        tasks.sort(Comparator.comparingLong(RunningTask::startMillis).thenComparing(RunningTask::key));

        for (RunningTask task : tasks) {
            if (!running.containsKey(task.key())) {
                continue; // 本 tick 内已被取消/禁用移除
            }
            Plan plan = plans.get(task.planId());
            Step step = plan.getStep(task.stepId());
            if (step.getState() != StepState.RUNNING) {
                running.remove(task.key());
                occupiedBy.remove(task.robotId());
                continue;
            }

            boolean timedOut = task.timeoutAtMillis() > 0 && now >= task.timeoutAtMillis();
            boolean due = now >= task.dueMillis();
            if (!timedOut && !due) {
                continue;
            }

            running.remove(task.key());
            occupiedBy.remove(task.robotId());

            if (timedOut) {
                boolean retry = step.handleTimeout();
                record(retry ? "TIMEOUT_RETRY" : "TIMEOUT_FINAL",
                        plan.getId(), step.getId(), task.robotId(),
                        "attempt=" + step.getAttempt() + ", retry=" + retry);
                log.warn("[settle] plan={} step={} robot={} TIMEOUT, retry={}",
                        plan.getId(), step.getId(), task.robotId(), retry);
            } else if (step.isAlwaysFail()) {
                boolean retry = step.handleFailure();
                record(retry ? "FAILED_RETRY" : "FAILED_FINAL",
                        plan.getId(), step.getId(), task.robotId(),
                        "attempt=" + step.getAttempt() + ", retry=" + retry);
                log.warn("[settle] plan={} step={} robot={} FAILED, retry={}",
                        plan.getId(), step.getId(), task.robotId(), retry);
            } else {
                boolean applied = step.markSucceeded();
                record("SUCCEEDED", plan.getId(), step.getId(), task.robotId(),
                        "applied=" + applied + ", attempt=" + step.getAttempt());
                log.info("[settle] plan={} step={} robot={} SUCCEEDED (effect applied={})",
                        plan.getId(), step.getId(), task.robotId(), applied);
            }

            PlanState before = plan.getState();
            PlanState after = plan.refreshState();
            if (after == PlanState.FAILED_FINAL && before != PlanState.FAILED_FINAL) {
                // 失败终态：释放同计划其余运行任务，未完成步骤级联取消，已完成保留
                releasePlanResources(plan);
                plan.cascadeCancelAfterFailure();
                record("PLAN_FAIL_CASCADE", plan.getId(), null, null,
                        "触发步骤=" + task.stepId() + "，已完成步骤保留");
                log.warn("[settle] plan={} FAILED_FINAL, other unfinished steps cancelled",
                        plan.getId());
            } else if (after == PlanState.COMPLETED) {
                log.info("[settle] plan={} COMPLETED", plan.getId());
                record("PLAN_COMPLETED", plan.getId(), null, null, "全部步骤成功");
            }
        }
    }

    // ---------------------------------------------------------------------
    // 只读视图（快照拷贝）
    // ---------------------------------------------------------------------

    public Plan getPlan(String planId) {
        lock.lock();
        try {
            return plans.get(planId);
        } finally {
            lock.unlock();
        }
    }

    public Robot getRobot(String robotId) {
        lock.lock();
        try {
            return robots.get(robotId);
        } finally {
            lock.unlock();
        }
    }

    public List<Plan> listPlans() {
        lock.lock();
        try {
            return List.copyOf(plans.values());
        } finally {
            lock.unlock();
        }
    }

    public List<Robot> listRobots() {
        lock.lock();
        try {
            return List.copyOf(robots.values());
        } finally {
            lock.unlock();
        }
    }

    public List<ScheduleDecision> decisions() {
        lock.lock();
        try {
            return List.copyOf(decisions);
        } finally {
            lock.unlock();
        }
    }

    /** 空闲时为 true：没有运行中任务，也没有任何未结束计划。 */
    public boolean isQuiet() {
        lock.lock();
        try {
            if (!running.isEmpty()) {
                return false;
            }
            for (Plan plan : plans.values()) {
                if (!plan.isFinished()) {
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void record(String action, String planId, String stepId, String robotId, String detail) {
        decisions.add(new ScheduleDecision(clock.millis(), action, planId, stepId, robotId, detail));
    }

    private PlanValidationException reject(RejectionReason reason, String message) {
        log.warn("[reject] reason={} detail={}", reason, message);
        return new PlanValidationException(reason, message);
    }
}
