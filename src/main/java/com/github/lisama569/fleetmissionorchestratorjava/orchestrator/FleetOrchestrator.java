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
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineKind;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.PlanValidator;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /** 工作线程串行消费的命令事件（提交/追加/取消/机器人下线等外部调用）。 */
    private record Event(long seq, String tag, Runnable action) {}

    /** 一次任务尝试完成后投递的结果。 */
    private record FinishRecord(String planId, String taskId, long launchId,
                                int attemptNumber, String robotId, long startTick,
                                AttemptOutcome outcome) {}

    private record RunningTask(String planId, TaskSpec spec, TaskRuntime runtime,
                               Future<?> future, String robotId, long startTick, long launchId) {}

    private record Dispatch(String planId, String taskId, String robotId, long startTick) {}

    private final FleetState state;
    private final PlanValidator validator = new PlanValidator();
    private final Scheduler scheduler = new Scheduler();
    private final TaskExecutionService executionService;
    private final Object lock = new Object();

    private final LinkedBlockingQueue<Event> commands = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<FinishRecord> finishEvents = new LinkedBlockingQueue<>();
    private final ExecutorService taskPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "task-exec");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, CompletableFuture<PlanSnapshot>> waiters = new ConcurrentHashMap<>();
    private final AtomicLong eventSeq = new AtomicLong();
    private final AtomicLong launchSeq = new AtomicLong();

    private final Map<String, RunningTask> running = new LinkedHashMap<>();
    private long tick;
    private volatile boolean workerRunning;
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
            if (workerRunning) {
                return;
            }
            workerRunning = true;
            workerThread = new Thread(this::workerLoop, "fleet-worker");
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    public void shutdown() {
        synchronized (lock) {
            workerRunning = false;
            for (RunningTask rt : new ArrayList<>(running.values())) {
                rt.future().cancel(true);
            }
            running.clear();
        }
        if (workerThread != null) {
            workerThread.interrupt();
        }
        taskPool.shutdownNow();
    }

    private void workerLoop() {
        while (true) {
            try {
                // 优先处理“已经到齐的完成梯队”：它代表模拟时间确定地向前推进。
                // 判定与开闸在同一把锁内一次完成，避免完成事件恰在两步之间到达造成误判。
                if (tryApplyReadyFinishCohort()) {
                    continue;
                }
                Event event = commands.poll(5, TimeUnit.MILLISECONDS);
                if (event == null) {
                    // 没有就绪完成事件、也没有命令：可能有完成事件在路上（等梯队到齐），短暂等待。
                    if (!finishEvents.isEmpty()) {
                        try {
                            Thread.sleep(2L);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        continue;
                    }
                    synchronized (lock) {
                        if (!workerRunning) {
                            return;
                        }
                    }
                    continue;
                }
                // 命令批处理：把此刻已在队列中的命令（如同批并发提交）整体取出，
                // 按 seq 顺序全部应用后再统一 pump，使它们在同一次调度决策中互相可见。
                List<Event> batch = new ArrayList<>();
                batch.add(event);
                commands.drainTo(batch);
                batch.sort(Comparator.comparingLong(Event::seq));
                for (Event cmd : batch) {
                    LOG.info("[batch#{}] {}", cmd.seq(), cmd.tag());
                    try {
                        cmd.action().run();
                    } catch (Throwable t) {
                        LOG.error("worker command failed: {}", cmd.tag(), t);
                    }
                }
                pump();
            } catch (Throwable t) {
                LOG.error("worker loop failure", t);
            }
        }
    }

    /**
     * 若完成队列中存在一个“其同梯队已全部到齐”的完成梯队，则一次性落地该梯队并 pump。
     * 同梯队 = 逻辑结束时刻等于队列中最早结束时刻的所有（未被抢占的）在跑任务。
     *
     * @return true 表示处理了一个梯队（无论其中是否有有效事件）；false 表示梯队尚未到齐
     */
    private boolean tryApplyReadyFinishCohort() {
        List<FinishRecord> cohort = new ArrayList<>();
        List<FinishRecord> later = new ArrayList<>();
        synchronized (lock) {
            if (finishEvents.isEmpty()) {
                return false;
            }
            finishEvents.drainTo(later);

            long earliestEnd = Long.MAX_VALUE;
            for (FinishRecord r : later) {
                earliestEnd = Math.min(earliestEnd, endTickOf(r));
            }
            // 若存在“结束时刻 <= earliestEnd 但完成事件尚未到达”的在跑任务，梯队未到齐：
            // 全部放回，等更多完成事件。
            for (RunningTask rt : running.values()) {
                long end = rt.startTick() + rt.spec().durationTicks();
                if (end <= earliestEnd && !hasLaunch(later, rt.launchId())) {
                    finishEvents.addAll(later);
                    return false;
                }
            }
            for (FinishRecord r : later) {
                if (endTickOf(r) <= earliestEnd) {
                    cohort.add(r);
                }
            }
            // 更晚的事件留在队列中。
            for (FinishRecord r : later) {
                if (endTickOf(r) > earliestEnd) {
                    finishEvents.add(r);
                }
            }
            // 陈旧事件（被抢占后旧 launchId 的迟到通知）直接剔除。
            cohort.removeIf(r -> {
                RunningTask self = running.get(Scheduler.key(r.planId(), r.taskId()));
                boolean stale = self == null || self.launchId() != r.launchId();
                if (stale) {
                    LOG.info("stale finish dropped at cohort plan={} task={} launchId={}",
                            r.planId(), r.taskId(), r.launchId());
                }
                return stale;
            });
            if (cohort.isEmpty()) {
                // 全是陈旧事件：已清理，继续推进（可能要立刻派发或处理下一梯队）。
                return true;
            }
            cohort.sort(Comparator.comparing(FinishRecord::planId)
                    .thenComparing(FinishRecord::taskId)
                    .thenComparingLong(FinishRecord::launchId));
            LOG.info("finish cohort closes at tick={} cohort={}", earliestEnd,
                    cohort.stream().map(r -> r.planId() + "/" + r.taskId()).toList());
            for (FinishRecord r : cohort) {
                onAttemptFinished(r);
            }
            pump();
            return true;
        }
    }

    private boolean hasLaunch(List<FinishRecord> records, long launchId) {
        for (FinishRecord r : records) {
            if (r.launchId() == launchId) {
                return true;
            }
        }
        return false;
    }

    private long endTickOf(FinishRecord rec) {
        if (rec.outcome() == AttemptOutcome.PREEMPTED) {
            return rec.startTick();
        }
        Plan plan = state.plans().get(rec.planId());
        return rec.startTick() + plan.task(rec.taskId()).durationTicks();
    }

    /** 外部线程提交命令事件并同步等待其被工作线程处理。 */
    private void call(String tag, Runnable action) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        long seq = eventSeq.incrementAndGet();
        commands.add(new Event(seq, tag, () -> {
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

    /** 任务执行线程投递完成事件到完成队列（不等待）。 */
    private void emitFinish(FinishRecord rec) {
        finishEvents.add(rec);
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

    // ------------------------------------------------------------------
    // 运行中追加步骤
    // ------------------------------------------------------------------

    /**
     * 向一份已接受且尚未收敛的计划整体追加一批新步骤。
     * 原子性：整批成功或整批不做。下列情况各自给出可区分原因并整体拒绝，
     * 拒绝时原计划继续照常执行，已完成步骤与在跑步骤均不受影响：
     *  - INVALID_APPEND                新批次为空
     *  - DUPLICATE_TASK_ID_IN_APPEND   新批次内部编号重复
     *  - TASK_ID_ALREADY_EXISTS        新编号与已接受步骤冲突（已接受步骤不可改动/顶替）
     *  - MISSING_DEPENDENCY            新步骤依赖了合并计划中不存在的步骤
     *  - CYCLIC_DEPENDENCY             合并后依赖图成环
     *  - UNKNOWN_REQUIRED_ROBOT        绑定了不存在的机器人
     *  - PLAN_NOT_APPENDABLE           目标计划不存在或已收敛为终态
     *  - NO_CAPABLE_ROBOT / RESOURCE_CONFLICT 合并后不存在可行排程
     *
     * @return 追加后计划的最新快照
     */
    public PlanSnapshot appendTasks(String planId, List<TaskSpec> appended) {
        call("append " + planId + " +" + (appended == null ? 0 : appended.size()),
                () -> acceptAppend(planId, appended));
        return snapshot(planId);
    }

    public PlanSnapshot appendTasksAndWait(String planId, List<TaskSpec> appended) {
        call("append " + planId + " +" + (appended == null ? 0 : appended.size()),
                () -> acceptAppend(planId, appended));
        return awaitPlan(planId);
    }

    private void acceptAppend(String planId, List<TaskSpec> appended) {
        synchronized (lock) {
            Plan existing = state.plans().get(planId);
            if (existing == null) {
                throw new PlanRejectedException(RejectionReason.PLAN_NOT_APPENDABLE,
                        "no such plan: " + planId);
            }
            if (isTerminal(planId)) {
                throw new PlanRejectedException(RejectionReason.PLAN_NOT_APPENDABLE,
                        "plan " + planId + " is already " + state.planStatus(planId)
                                + "; tasks cannot be appended to a converged plan");
            }

            // 1) 静态校验（批次内重复 / 与已接受冲突 / 缺依赖 / 成环 / 未知机器人）。
            //    merged 只是本地只读对象；在走到写状态之前抛出异常，原计划一律不受影响。
            Plan merged = validator.validateAppend(existing, appended, state.robots());

            // 2) 对合并后的整体做确定性可行性排程：已完成工作保留、在跑工作为固定占用。
            List<Plan> accepted = new ArrayList<>(state.plans().values());
            // 用合并计划临时替换本计划参与排程（runtimes 仍来自 state，已完成/在跑状态不变）。
            accepted.removeIf(p -> p.planId().equals(planId));
            accepted.add(merged);
            Schedule schedule = scheduler.reschedule(merged, state.robots(), accepted,
                    state.flatRuntimes(), tick);
            if (!schedule.feasible()) {
                LOG.info("append rejected planId={} reason={}", planId, schedule.reason());
                throw new PlanRejectedException(schedule.reason(),
                        "append into plan " + planId + " not feasible: " + schedule.reason());
            }

            // 3) 全部通过后才提交：替换计划、为新步骤建运行时（旧步骤运行时原样保留）。
            state.replacePlan(merged, tick);
            for (TaskAssignment a : schedule.assignments()) {
                boolean isNew = appended.stream().anyMatch(t -> t.taskId().equals(a.taskId()));
                if (isNew) {
                    state.log(planId, a.startTick(), "SCHEDULE-APPEND",
                            "appended task " + a.taskId() + " -> robot " + a.robotId()
                                    + " [" + a.startTick() + "," + a.endTick() + ")");
                }
            }
            state.log(planId, tick, "APPEND",
                    "accepted " + appended.size() + " new task(s): "
                            + appended.stream().map(TaskSpec::taskId).toList());
            state.timeline(planId, tick, "", "", TimelineKind.APPEND, 0,
                    "accepted " + appended.size() + " new task(s): "
                            + appended.stream().map(TaskSpec::taskId).toList());
            LOG.info("append accepted plan={} newTasks={} tick={}",
                    planId, appended.stream().map(TaskSpec::taskId).toList(), tick);
        }
    }

    public PlanSnapshot awaitPlan(String planId) {        CompletableFuture<PlanSnapshot> future;
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

    /** 全局执行时间线（跨计划），按时间先后确定性排序，供复盘与排查。 */
    public List<com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent> timeline() {
        synchronized (lock) {
            return state.timeline();
        }
    }

    /** 单个计划的执行时间线。 */
    public List<com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent>
    timeline(String planId) {
        synchronized (lock) {
            return state.timeline(planId);
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
                for (RunningTask victim : runningOnRobot(robotId)) {
                    preempt(victim, "robot " + robotId + " unavailable");
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
                RunningTask victim = running.get(Scheduler.key(planId, taskId));
                if (victim != null) {
                    preempt(victim, "task " + taskId + " cancelled");
                }
                if (!rt.status().isTerminal()) {
                    rt.transition(TaskStatus.CANCELLED, AttemptOutcome.PREEMPTED,
                            rt.assignedRobotId(), tick);
                    state.log(planId, tick, "CANCEL", "task " + taskId + " cancelled");
                    state.timeline(planId, tick, taskId, rt.assignedRobotId(),
                            TimelineKind.CANCEL, 0, "task cancelled");
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

                // 已经被在跑任务占用的机器人：同一台机器人同一时刻只能干一件事。
                Set<String> busyRobots = new HashSet<>();
                for (RunningTask rt : running.values()) {
                    busyRobots.add(rt.robotId());
                }

                List<Plan> activePlans = activePlans();
                if (activePlans.isEmpty()) {
                    return;
                }

                // 为每个活动计划计算确定性调度，跨计划选出当前 tick 可启动的最高优先任务。
                Dispatch best = null;
                for (Plan plan : activePlans) {
                    Schedule sched = scheduler.reschedule(plan, state.robots(),
                            activePlans, state.flatRuntimes(), tick);
                    if (!sched.feasible()) {
                        failInfeasible(plan, sched);
                        continue;
                    }
                    Dispatch d = nextDispatch(plan, sched, busyRobots);
                    if (d != null && (best == null
                            || higherPriority(d, best, plan))) {
                        best = d;
                    }
                }
                if (best == null) {
                    // 没有当前 tick 能启动的任务：等待某个在跑任务的完成事件。
                    return;
                }
                // 派发后不退出循环：调度器会把新派任务视为对其机器人的固定占用，
                // 于是彼此无依赖、机器人条件也够的任务可在同一 tick 铺到多台机器人上并行。
                launch(best);
                busyRobots.add(best.robotId());
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

    private Dispatch nextDispatch(Plan plan, Schedule sched, Set<String> busyRobots) {
        for (TaskAssignment a : sched.assignments()) {
            TaskRuntime rt = state.taskRuntimes(plan.planId()).get(a.taskId());
            if (rt == null || rt.status() != TaskStatus.PENDING || a.robotId() == null) {
                continue;
            }
            if (busyRobots.contains(a.robotId())) {
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
        running.put(Scheduler.key(d.planId(), d.taskId()),
                new RunningTask(d.planId(), spec, rt, future, d.robotId(), startTick, launchId));
        state.timeline(d.planId(), startTick, d.taskId(), d.robotId(),
                TimelineKind.DISPATCH, attemptNumber,
                "dispatched attempt " + attemptNumber + " to robot " + d.robotId());
    }

    private void onAttemptFinished(FinishRecord rec) {
        synchronized (lock) {
            String key = Scheduler.key(rec.planId(), rec.taskId());
            RunningTask task = running.get(key);
            TaskRuntime rt = state.taskRuntimes(rec.planId()).get(rec.taskId());
            Plan plan = state.plans().get(rec.planId());
            TaskSpec spec = plan.task(rec.taskId());

            // 用唯一 launchId 判定陈旧：被抢占后同一任务可能已重新启动（新的 launchId），
            // 旧尝试的完成事件必须被忽略，不能影响当前状态。
            if (task == null || task.launchId() != rec.launchId()) {
                LOG.info("stale finish ignored plan={} task={} launchId={}",
                        rec.planId(), rec.taskId(), rec.launchId());
                return;
            }
            running.remove(key);

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
                        state.timeline(rec.planId(), tick, rec.taskId(), rec.robotId(),
                                TimelineKind.COMPLETE, rec.attemptNumber(),
                                "succeeded after " + rec.attemptNumber() + " attempt(s)");
                        LOG.info("task succeeded plan={} task={} attempts={} tick={}",
                                rec.planId(), rec.taskId(), rec.attemptNumber(), tick);
                    }
                    case PREEMPTED -> {
                        // 通常抢占已在命令事件中复位；兜底处理（中断极快到达的情形）。
                        rt.markPreempted();
                        state.log(rec.planId(), tick, "PREEMPT",
                                "task " + rec.taskId() + " preempted, attempt "
                                        + rec.attemptNumber() + " not counted, will reassign");
                        state.timeline(rec.planId(), tick, rec.taskId(), rec.robotId(),
                                TimelineKind.PREEMPT, rec.attemptNumber(),
                                "preempted, attempt not counted, will reassign");
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
                            state.timeline(rec.planId(), tick, rec.taskId(), rec.robotId(),
                                    TimelineKind.EXHAUSTED, rec.attemptNumber(),
                                    terminal + " after " + rec.attemptNumber() + " attempt(s)");
                            LOG.info("task exhausted plan={} task={} status={} tick={}",
                                    rec.planId(), rec.taskId(), terminal, tick);
                            cascadeDownstream(rec.planId());
                        } else {
                            rt.resetForRetry(rec.robotId());
                            state.log(rec.planId(), tick, "RETRY",
                                    "task " + rec.taskId() + " " + rec.outcome()
                                            + ", scheduling retry (" + rec.attemptNumber()
                                            + "/" + spec.maxAttempts() + " used)");
                            state.timeline(rec.planId(), tick, rec.taskId(), rec.robotId(),
                                    TimelineKind.RETRY, rec.attemptNumber(),
                                    rec.outcome() + ", scheduling retry ("
                                            + rec.attemptNumber() + "/" + spec.maxAttempts()
                                            + " used)");
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

    /** 当前正占用指定机器人的在跑任务（确定性：按 key 字典序）。 */
    private List<RunningTask> runningOnRobot(String robotId) {
        return running.values().stream()
                .filter(rt -> rt.robotId().equals(robotId))
                .sorted(Comparator.comparing(rt -> Scheduler.key(rt.planId(), rt.spec().taskId())))
                .toList();
    }

    private void preempt(RunningTask victim, String reason) {
        if (victim == null) {
            return;
        }
        state.log(victim.planId(), tick, "INTERRUPT",
                "interrupting task " + victim.spec().taskId() + ": " + reason);
        LOG.info("preempt plan={} task={} robot={} reason={}",
                victim.planId(), victim.spec().taskId(), victim.robotId(), reason);
        victim.future().cancel(true);
        // 立即复位：释放机器人、回到 PENDING、被打断的尝试不计次数；
        // 之后该任务可被立刻重新分配。稍后到达的旧完成事件会因 launchId 不匹配而被忽略。
        victim.runtime().markPreempted();
        running.remove(Scheduler.key(victim.planId(), victim.spec().taskId()));
        state.timeline(victim.planId(), tick, victim.spec().taskId(), victim.robotId(),
                TimelineKind.PREEMPT, victim.runtime().attempts() + 1,
                "preempted: " + reason);
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
                        RunningTask victim = running.get(Scheduler.key(planId, t.taskId()));
                        if (victim != null) {
                            preempt(victim, "downstream of " + dep + " " + ds);
                        }
                        if (!rt.status().isTerminal()) {
                            rt.transition(TaskStatus.SKIPPED, null, rt.assignedRobotId(), tick);
                            state.log(planId, tick, "SKIP",
                                    "task " + t.taskId() + " skipped because dependency "
                                            + dep + " is " + ds);
                            state.timeline(planId, tick, t.taskId(), rt.assignedRobotId(),
                                    TimelineKind.SKIP, 0,
                                    "skipped because dependency " + dep + " is " + ds);
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
            state.timeline(plan.planId(), tick, "", "", TimelineKind.REPLAN, 0, trigger);
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
