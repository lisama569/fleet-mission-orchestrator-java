package com.github.lisama569.fleetmissionorchestratorjava.orchestrator;

import com.github.lisama569.fleetmissionorchestratorjava.execution.TaskExecutionService;
import com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException;
import com.github.lisama569.fleetmissionorchestratorjava.model.AttemptOutcome;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.Schedule;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskAssignment;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntime;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.PlanValidator;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 车队任务编排门面（线程安全）。
 *
 * 并发性模型：外部线程只把“命令事件”放入队列；任务执行线程在结束时投递“完成事件”；
 * 单工作线程串行消费所有事件、推进模拟时钟、做调度与状态迁移。
 * 因而并发提交的结果稳定一致，相同输入序列下的调度与重规划结论可复现。
 */
public final class FleetOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(FleetOrchestrator.class);

    /** 工作线程串行消费的事件。 */
    private record Event(long seq, String tag, Runnable action) {}

    /** 一次任务尝试完成后投递的结果。 */
    private record FinishRecord(String planId, String taskId, long launchId,
                                int attemptNumber, String robotId, long startTick,
                                AttemptOutcome outcome) {}

    private record RunningTask(String planId, TaskSpec spec, TaskRuntime runtime,
                               Future<?> future, long startTick, long launchId) {}

    private record Dispatch(String planId, String taskId, String robotId, long startTick) {}

    private final FleetState state;
    private final PlanValidator validator = new PlanValidator();
    private final Scheduler scheduler = new Scheduler();
    private final TaskExecutionService executionService;
    private final Object lock = new Object();

    private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final ExecutorService taskPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "task-exec");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, CompletableFuture<PlanSnapshot>> waiters = new ConcurrentHashMap<>();
    private final AtomicLong eventSeq = new AtomicLong();
    private final AtomicLong launchSeq = new AtomicLong();

    private RunningTask current;
    private long tick;
    private volatile boolean running;
    private Thread workerThread;

    public FleetOrchestrator(List<Robot> robots, long millisPerTick) {
        this.state = new FleetState(robots);
        this.executionService = new TaskExecutionService(millisPerTick);
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    public void start() {
        synchronized (lock) {
            if (running) {
                return;
            }
            running = true;
            workerThread = new Thread(this::workerLoop, "fleet-worker");
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    public void shutdown() {
        synchronized (lock) {
            running = false;
            if (current != null) {
                current.future().cancel(true);
            }
        }
        if (workerThread != null) {
            workerThread.interrupt();
        }
        taskPool.shutdownNow();
    }

    private void workerLoop() {
        while (true) {
            Event event;
            try {
                event = events.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (event == null) {
                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                }
                continue;
            }
            LOG.info("[in#{}] {}", event.seq(), event.tag());
            try {
                event.action().run();
            } catch (Throwable t) {
                LOG.error("worker event failed: {}", event.tag(), t);
            }
            try {
                pump();
            } catch (Throwable t) {
                LOG.error("pump failed", t);
            }
        }
    }

    /** 外部线程提交命令事件并同步等待其被工作线程处理。 */
    private void call(String tag, Runnable action) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        long seq = eventSeq.incrementAndGet();
        events.add(new Event(seq, tag, () -> {
            try {
                action.run();
                done.complete(null);
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
            }
        }));
        try {
            done.join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    /** 任务执行线程投递完成事件（不等待）。 */
    private void emitFinish(FinishRecord rec) {
        long seq = eventSeq.incrementAndGet();
        events.add(new Event(seq, "finish " + rec.planId() + "/" + rec.taskId(),
                () -> onAttemptFinished(rec)));
    }

    // ------------------------------------------------------------------
    // 提交计划
    // ------------------------------------------------------------------

    public String submit(Plan plan) {
        call("submit " + plan.planId(), () -> acceptPlan(plan));
        return plan.planId();
    }

    public PlanSnapshot submitAndWait(Plan plan) {
        submit(plan);
        return awaitPlan(plan.planId());
    }

    private void acceptPlan(Plan plan) {
        synchronized (lock) {
            if (state.plans().containsKey(plan.planId())) {
                throw new PlanRejectedException(RejectionReason.DUPLICATE_PLAN_ID,
                        "plan already exists: " + plan.planId());
            }
            validator.validate(plan, state.robots());

            List<Plan> accepted = new ArrayList<>(state.plans().values());
            Schedule schedule = scheduler.reschedule(plan, state.robots(), accepted,
                    state.flatRuntimes(), tick);
            if (!schedule.feasible()) {
                LOG.info("plan rejected planId={} reason={}", plan.planId(), schedule.reason());
                throw new PlanRejectedException(schedule.reason(),
                        "plan " + plan.planId() + " not feasible: " + schedule.reason());
            }

            state.registerPlan(plan, tick);
            state.setPlanStatus(plan.planId(), PlanStatus.EXECUTING, tick);
            for (TaskAssignment a : schedule.assignments()) {
                state.log(plan.planId(), a.startTick(), "SCHEDULE",
                        "task " + a.taskId() + " -> robot " + a.robotId()
                                + " [" + a.startTick() + "," + a.endTick() + ")");
            }
            LOG.info("plan accepted planId={} arrivalTick={} assignments={}",
                    plan.planId(), plan.arrivalTick(), schedule.assignments().size());
        }
    }

    public PlanSnapshot awaitPlan(String planId) {
        CompletableFuture<PlanSnapshot> future;
        synchronized (lock) {
            if (isTerminal(planId)) {
                return state.snapshot(planId);
            }
            future = waiters.computeIfAbsent(planId, k -> new CompletableFuture<>());
        }
        return future.join();
    }

    public PlanSnapshot snapshot(String planId) {
        synchronized (lock) {
            return state.snapshot(planId);
        }
    }

    // ------------------------------------------------------------------
    // 动态重规划
    // ------------------------------------------------------------------

    public void robotUnavailable(String robotId, long atTick) {
        call("robot-unavailable " + robotId, () -> {
            synchronized (lock) {
                Robot r = state.robots().get(robotId);
                if (r == null) {
                    throw new IllegalArgumentException("unknown robot: " + robotId);
                }
                state.robots().put(robotId, r.offline());
                LOG.info("robot marked unavailable robot={} atTick={}", robotId, tick);
                if (current != null && robotId.equals(current.runtime().assignedRobotId())) {
                    preemptCurrent("robot " + robotId + " unavailable");
                }
                replanAll("robot-unavailable:" + robotId);
            }
        });
    }

    public void cancelTask(String planId, String taskId, long atTick) {
        call("cancel " + planId + "/" + taskId, () -> {
            synchronized (lock) {
                TaskRuntime rt = state.taskRuntimes(planId).get(taskId);
                if (rt == null) {
                    throw new IllegalArgumentException("no such task: " + planId + "/" + taskId);
                }
                if (rt.status() == TaskStatus.SUCCEEDED) {
                    LOG.info("cancel ignored: task already succeeded task={}/{}", planId, taskId);
                    return;
                }
                if (current != null && current.planId().equals(planId)
                        && current.spec().taskId().equals(taskId)) {
                    preemptCurrent("task " + taskId + " cancelled");
                }
                if (!rt.status().isTerminal()) {
                    rt.transition(TaskStatus.CANCELLED, AttemptOutcome.PREEMPTED,
                            rt.assignedRobotId(), tick);
                    state.log(planId, tick, "CANCEL", "task " + taskId + " cancelled");
                }
                cascadeDownstream(planId);
                replanAll("cancel:" + planId + ":" + taskId);
                checkPlanTerminal(planId);
            }
        });
    }

    public String insertHighPriority(Plan plan, long atTick) {
        call("high-priority " + plan.planId(), () -> {
            acceptPlan(plan);
            synchronized (lock) {
                replanAll("high-priority:" + plan.planId());
            }
        });
        return plan.planId();
    }

    public void cancelTask(String planId, String taskId) {
        cancelTask(planId, taskId, currentTick());
    }

    public void robotUnavailable(String robotId) {
        robotUnavailable(robotId, currentTick());
    }

    public long currentTick() {
        synchronized (lock) {
            return tick;
        }
    }

    // ------------------------------------------------------------------
    // 核心推进：始终在工作线程、持锁状态下执行
    // ------------------------------------------------------------------

    private void pump() {
        synchronized (lock) {
            while (true) {
                propagateDependencies();

                // 当前任务仍在执行：等其完成事件，不启动新任务（同一资源不重复占用）。
                if (current != null) {
                    return;
                }

                List<Plan> activePlans = activePlans();
                if (activePlans.isEmpty()) {
                    return;
                }

                // 为每个活动计划计算确定性调度，选取当前 tick 可启动的最高优先任务。
                Dispatch best = null;
                for (Plan plan : activePlans) {
                    Schedule sched = scheduler.reschedule(plan, state.robots(),
                            activePlans, state.flatRuntimes(), tick);
                    if (!sched.feasible()) {
                        failInfeasible(plan, sched);
                        continue;
                    }
                    Dispatch d = nextDispatch(plan, sched);
                    if (d != null && (best == null
                            || higherPriority(d, best, plan))) {
                        best = d;
                    }
                }
                if (best == null) {
                    return;
                }
                launch(best);
                return;
            }
        }
    }

    /** 跨计划选择：优先级数值高者优先；相同优先级按 (planId, taskId) 字典序，保证确定。 */
    private boolean higherPriority(Dispatch candidate, Dispatch incumbent, Plan plan) {
        TaskSpec cs = plan.task(candidate.taskId());
        Plan ip = state.plans().get(incumbent.planId());
        TaskSpec is = ip.task(incumbent.taskId());
        if (cs.priority() != is.priority()) {
            return cs.priority() > is.priority();
        }
        int pc = candidate.planId().compareTo(incumbent.planId());
        if (pc != 0) {
            return pc < 0;
        }
        return candidate.taskId().compareTo(incumbent.taskId()) < 0;
    }

    private Dispatch nextDispatch(Plan plan, Schedule sched) {
        for (TaskAssignment a : sched.assignments()) {
            TaskRuntime rt = state.taskRuntimes(plan.planId()).get(a.taskId());
            if (rt == null || rt.status() != TaskStatus.PENDING || a.robotId() == null) {
                continue;
            }
            if (a.startTick() <= tick) {
                return new Dispatch(plan.planId(), a.taskId(), a.robotId(), a.startTick());
            }
        }
        return null;
    }

    private void launch(Dispatch d) {
        Plan plan = state.plans().get(d.planId());
        TaskSpec spec = plan.task(d.taskId());
        TaskRuntime rt = state.taskRuntimes(d.planId()).get(d.taskId());
        long startTick = tick;
        rt.markRunning(d.robotId(), startTick);
        int attemptNumber = rt.attempts();
        state.log(d.planId(), startTick, "DISPATCH",
                "task " + d.taskId() + " attempt " + attemptNumber
                        + " -> robot " + d.robotId());
        LOG.info("dispatch plan={} task={} attempt={} robot={} tick={}",
                d.planId(), d.taskId(), attemptNumber, d.robotId(), startTick);

        long launchId = launchSeq.incrementAndGet();
        Future<?> future = taskPool.submit(() -> {
            AttemptOutcome outcome;
            try {
                outcome = executionService.executeAttempt(spec, attemptNumber, startTick);
            } catch (Throwable t) {
                outcome = AttemptOutcome.FAILURE;
                LOG.error("execution threw for task {}", spec.taskId(), t);
            }
            emitFinish(new FinishRecord(d.planId(), d.taskId(), launchId, attemptNumber,
                    d.robotId(), startTick, outcome));
        });
        current = new RunningTask(d.planId(), spec, rt, future, startTick, launchId);
    }

    private void onAttemptFinished(FinishRecord rec) {
        synchronized (lock) {
            TaskRuntime rt = state.taskRuntimes(rec.planId()).get(rec.taskId());
            Plan plan = state.plans().get(rec.planId());
            TaskSpec spec = plan.task(rec.taskId());

            // 用唯一 launchId 判定陈旧：被抢占后同一任务可能已重新启动（新的 launchId），
            // 旧尝试的完成事件必须被忽略，不能影响当前状态。
            boolean stale = current == null
                    || current.launchId() != rec.launchId();
            if (stale) {
                LOG.info("stale finish ignored plan={} task={} launchId={}",
                        rec.planId(), rec.taskId(), rec.launchId());
                return;
            }
            current = null;

            long finishTick = rec.startTick() + spec.durationTicks();
            if (rec.outcome() == AttemptOutcome.PREEMPTED) {
                finishTick = Math.max(tick, rec.startTick());
            }
            tick = Math.max(tick, finishTick);

            // 抢占在中断落地前已把运行时复位为 PENDING：此事件不做任何状态迁移。
            if (rt.status() != TaskStatus.RUNNING) {
                LOG.info("finish after non-running state plan={} task={} status={} outcome={}",
                        rec.planId(), rec.taskId(), rt.status(), rec.outcome());
            } else {
                switch (rec.outcome()) {
                    case SUCCESS -> {
                        rt.transition(TaskStatus.SUCCEEDED, AttemptOutcome.SUCCESS,
                                rec.robotId(), tick);
                        state.log(rec.planId(), tick, "COMPLETE",
                                "task " + rec.taskId() + " succeeded after "
                                        + rec.attemptNumber() + " attempt(s)");
                        LOG.info("task succeeded plan={} task={} attempts={} tick={}",
                                rec.planId(), rec.taskId(), rec.attemptNumber(), tick);
                    }
                    case PREEMPTED -> {
                        // 通常抢占已在命令事件中复位；兜底处理（中断极快到达的情形）。
                        rt.markPreempted();
                        state.log(rec.planId(), tick, "PREEMPT",
                                "task " + rec.taskId() + " preempted, attempt "
                                        + rec.attemptNumber() + " not counted, will reassign");
                        LOG.info("task preempted plan={} task={} tick={}",
                                rec.planId(), rec.taskId(), tick);
                    }
                    case FAILURE, TIMEOUT -> {
                        TaskStatus terminal = rec.outcome() == AttemptOutcome.TIMEOUT
                                ? TaskStatus.TIMED_OUT : TaskStatus.FAILED;
                        if (rec.attemptNumber() >= spec.maxAttempts()) {
                            rt.transition(terminal, rec.outcome(), rec.robotId(), tick);
                            state.log(rec.planId(), tick, "EXHAUSTED",
                                    "task " + rec.taskId() + " " + terminal + " after "
                                            + rec.attemptNumber() + " attempt(s)");
                            LOG.info("task exhausted plan={} task={} status={} tick={}",
                                    rec.planId(), rec.taskId(), terminal, tick);
                            cascadeDownstream(rec.planId());
                        } else {
                            rt.resetForRetry(rec.robotId());
                            state.log(rec.planId(), tick, "RETRY",
                                    "task " + rec.taskId() + " " + rec.outcome()
                                            + ", scheduling retry (" + rec.attemptNumber()
                                            + "/" + spec.maxAttempts() + " used)");
                            LOG.info("task retry plan={} task={} outcome={} attempts={} tick={}",
                                    rec.planId(), rec.taskId(), rec.outcome(),
                                    rec.attemptNumber(), tick);
                        }
                    }
                }
            }
            checkPlanTerminal(rec.planId());
        }
    }

    private void preemptCurrent(String reason) {
        if (current != null) {
            RunningTask victim = current;
            state.log(victim.planId(), tick, "INTERRUPT",
                    "interrupting task " + victim.spec().taskId() + ": " + reason);
            LOG.info("preempt current plan={} task={} reason={}",
                    victim.planId(), victim.spec().taskId(), reason);
            victim.future().cancel(true);
            // 立即复位：释放机器人、回到 PENDING、被打断的尝试不计次数；
            // 之后该任务可被立刻重新分配。稍后到达的旧完成事件会因 launchId 不匹配而被忽略。
            victim.runtime().markPreempted();
            current = null;
        }
    }

    // ------------------------------------------------------------------
    // 依赖传播、终态判定
    // ------------------------------------------------------------------

    private void propagateDependencies() {
        for (Plan plan : activePlans()) {
            var runtimes = state.taskRuntimes(plan.planId());
            boolean changed = true;
            while (changed) {
                changed = false;
                for (TaskSpec t : plan.tasks()) {
                    TaskRuntime rt = runtimes.get(t.taskId());
                    if (rt.status() != TaskStatus.PENDING) {
                        continue;
                    }
                    for (String dep : t.dependencies()) {
                        TaskStatus ds = runtimes.get(dep).status();
                        if (ds != TaskStatus.SUCCEEDED && ds.isTerminal()) {
                            rt.transition(TaskStatus.SKIPPED, null, null, tick);
                            state.log(plan.planId(), tick, "SKIP",
                                    "task " + t.taskId() + " skipped because dependency "
                                            + dep + " is " + ds);
                            changed = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    private void cascadeDownstream(String planId) {
        Plan plan = state.plans().get(planId);
        var runtimes = state.taskRuntimes(planId);
        boolean changed = true;
        while (changed) {
            changed = false;
            for (TaskSpec t : plan.tasks()) {
                TaskRuntime rt = runtimes.get(t.taskId());
                if (rt.status() == TaskStatus.SUCCEEDED
                        || rt.status() == TaskStatus.SKIPPED
                        || rt.status() == TaskStatus.CANCELLED) {
                    continue;
                }
                for (String dep : t.dependencies()) {
                    TaskStatus ds = runtimes.get(dep).status();
                    if (ds.isTerminal() && ds != TaskStatus.SUCCEEDED) {
                        if (rt.status() == TaskStatus.RUNNING
                                && current != null && current.planId().equals(planId)
                                && current.spec().taskId().equals(t.taskId())) {
                            preemptCurrent("downstream of " + dep + " " + ds);
                        }
                        if (!rt.status().isTerminal()) {
                            rt.transition(TaskStatus.SKIPPED, null, rt.assignedRobotId(), tick);
                            state.log(planId, tick, "SKIP",
                                    "task " + t.taskId() + " skipped because dependency "
                                            + dep + " is " + ds);
                            changed = true;
                        }
                    }
                }
            }
        }
    }

    private void failInfeasible(Plan plan, Schedule sched) {
        // 把尚未到达终态的任务收敛为 SKIPPED，保证最终状态完整、确定、可解释。
        var runtimes = state.taskRuntimes(plan.planId());
        for (TaskSpec t : plan.tasks()) {
            TaskRuntime rt = runtimes.get(t.taskId());
            if (!rt.status().isTerminal()) {
                rt.transition(TaskStatus.SKIPPED, null, rt.assignedRobotId(), tick);
            }
        }
        state.log(plan.planId(), tick, "INFEASIBLE",
                "plan cannot continue: " + sched.reason());
        state.setPlanStatus(plan.planId(), PlanStatus.FAILED, tick);
        LOG.info("plan failed as infeasible plan={} reason={}", plan.planId(), sched.reason());
        completeWaiter(plan.planId());
    }

    private void replanAll(String trigger) {
        for (Plan plan : activePlans()) {
            state.log(plan.planId(), tick, "REPLAN", trigger);
        }
        LOG.info("replan triggered at tick={} trigger={}", tick, trigger);
    }

    private void checkPlanTerminal(String planId) {
        Plan plan = state.plans().get(planId);
        if (plan == null || isTerminal(planId)) {
            return;
        }
        var runtimes = state.taskRuntimes(planId);
        boolean allTerminal = plan.tasks().stream()
                .allMatch(t -> runtimes.get(t.taskId()).status().isTerminal());
        if (!allTerminal) {
            return;
        }
        boolean allOk = plan.tasks().stream()
                .allMatch(t -> runtimes.get(t.taskId()).status() == TaskStatus.SUCCEEDED);
        boolean allCancelled = plan.tasks().stream().allMatch(t -> {
            TaskStatus s = runtimes.get(t.taskId()).status();
            return s == TaskStatus.CANCELLED || s == TaskStatus.SKIPPED;
        });
        PlanStatus result = allOk ? PlanStatus.COMPLETED
                : allCancelled ? PlanStatus.CANCELLED : PlanStatus.FAILED;
        state.setPlanStatus(planId, result, tick);
        LOG.info("plan terminal plan={} status={} tick={}", planId, result, tick);
        state.log(planId, tick, "PLAN-" + result, "plan converged to " + result);
        completeWaiter(planId);
    }

    private void completeWaiter(String planId) {
        CompletableFuture<PlanSnapshot> w = waiters.remove(planId);
        if (w != null) {
            w.complete(state.snapshot(planId));
        }
    }

    private List<Plan> activePlans() {
        return state.plans().values().stream()
                .filter(p -> {
                    PlanStatus s = state.planStatus(p.planId());
                    return s == PlanStatus.EXECUTING || s == PlanStatus.ACCEPTED;
                })
                .sorted(Comparator.comparing(Plan::planId))
                .toList();
    }

    private boolean isTerminal(String planId) {
        PlanStatus s = state.planStatus(planId);
        return s != null && s.isTerminal();
    }

    /** 打印一份计划的输入、调度决策与最终状态。 */
    public String explain(String planId) {
        synchronized (lock) {
            Plan plan = state.plans().get(planId);
            PlanSnapshot snap = state.snapshot(planId);
            StringBuilder sb = new StringBuilder();
            sb.append("\n===== EXPLANATION plan=").append(planId).append(" status=")
                    .append(snap.status()).append(" =====\n");
            sb.append("input tasks:\n");
            for (var t : plan.tasks()) {
                sb.append(String.format("  %-6s type=%-8s dur=%d prio=%d deps=%s maxAttempts=%d%n",
                        t.taskId(), t.type(), t.durationTicks(), t.priority(),
                        t.dependencies(), t.maxAttempts()));
            }
            sb.append("decisions:\n");
            for (var e : snap.decisions()) {
                sb.append(String.format("  tick=%-4d [%-10s] %s%n",
                        e.tick(), e.category(), e.message()));
            }
            sb.append("final task states:\n");
            for (var t : snap.tasks()) {
                sb.append(String.format("  %-6s %-10s attempts=%d robot=%s outcome=%s%n",
                        t.taskId(), t.status(), t.attempts(),
                        t.assignedRobotId(), t.lastOutcome()));
            }
            String text = sb.toString();
            LOG.info("explain{}", text);
            return text;
        }
    }
}
