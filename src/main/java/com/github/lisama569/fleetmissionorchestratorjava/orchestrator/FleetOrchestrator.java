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
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEntry;
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

    /** 工作线程串行消费的事件。finish 非空表示这是一个任务完成事件（先入缓冲再按 tick 批处理）。 */
    private record Event(long seq, String tag, Runnable action, FinishRecord finish) {}

    /** 一次任务尝试完成后投递的结果。finishTick = startTick + durationTicks（模拟完成时刻）。 */
    private record FinishRecord(String planId, String taskId, long launchId,
                                int attemptNumber, String robotId, long startTick,
                                long finishTick, AttemptOutcome outcome) {}

    private record RunningTask(String planId, TaskSpec spec, TaskRuntime runtime,
                               Future<?> future, long startTick, long finishTick, long launchId) {}

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
    /**
     * 已发起但尚未被工作线程处理完的外部命令数。工作线程只在它为 0 时才做派发，
     * 保证“一波并发提交全部落地后”才基于完整计划集合做确定性并行派发，
     * 相同输入序列的最终状态与每个任务的落机/次数可复现。
     */
    private final java.util.concurrent.atomic.AtomicInteger inFlightCommands =
            new java.util.concurrent.atomic.AtomicInteger();

    /** 正在执行的任务，key = planId::taskId。同一台机器人在同一时刻只会出现在一个条目中。 */
    private final Map<String, RunningTask> runningTasks = new java.util.LinkedHashMap<>();
    /**
     * 已到达但尚未应用的完成事件。同一模拟时刻完成的并行任务在真实时间里先后到达，
     * 直接按到达顺序处理会让后续派发依赖线程调度。这里先缓冲，等“预期完成时刻不晚于
     * 缓冲中最早时刻”的所有在跑任务都报到了，再按 (finishTick, launchId) 成批应用，
     * 保证相同输入序列下处理顺序确定、最终状态可复现。
     */
    private final List<FinishRecord> finishBuffer = new ArrayList<>();
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
            for (RunningTask rt : runningTasks.values()) {
                rt.future().cancel(true);
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
            if (event.finish() != null) {
                synchronized (lock) {
                    finishBuffer.add(event.finish());
                }
            } else {
                try {
                    event.action().run();
                } catch (Throwable t) {
                    LOG.error("worker event failed: {}", event.tag(), t);
                }
            }
            try {
                drainAndPump();
            } catch (Throwable t) {
                LOG.error("drain/pump failed", t);
            }
        }
    }

    /**
     * 应用所有“已经到齐”的完成事件并触发派发。
     * 一批同 tick 完成的事件全部应用完后才 pump 一次；若还有外部命令在飞行中
     * （尚未入队/未处理完）则暂缓派发，等最后一条命令处理时再基于完整状态 pump。
     */
    private void drainAndPump() {
        synchronized (lock) {
            while (!finishBuffer.isEmpty()) {
                long tmin = finishBuffer.stream()
                        .mapToLong(FinishRecord::finishTick).min().orElseThrow();
                // 是否还有预期不晚于 tmin 完成、但完成事件尚未到达的在跑任务？
                boolean waiting = runningTasks.values().stream()
                        .filter(rt -> rt.finishTick() <= tmin)
                        .anyMatch(rt -> finishBuffer.stream()
                                .noneMatch(r -> r.launchId() == rt.launchId()));
                if (waiting) {
                    break;
                }
                List<FinishRecord> batch = finishBuffer.stream()
                        .filter(r -> r.finishTick() == tmin)
                        .sorted(Comparator.comparingLong(FinishRecord::launchId))
                        .toList();
                finishBuffer.removeAll(batch);
                for (FinishRecord rec : batch) {
                    onAttemptFinished(rec);
                }
                if (inFlightCommands.get() == 0) {
                    pump();
                }
            }
            if (inFlightCommands.get() == 0) {
                pump();
            }
        }
    }

    /** 外部线程提交命令事件并同步等待其被工作线程处理。 */
    private void call(String tag, Runnable action) {
        inFlightCommands.incrementAndGet();
        CompletableFuture<Void> done = new CompletableFuture<>();
        long seq = eventSeq.incrementAndGet();
        events.add(new Event(seq, tag, () -> {
            try {
                action.run();
                done.complete(null);
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
            } finally {
                // 在工作线程内、事件处理完之后递减：此刻起该命令不再算“在飞行中”。
                inFlightCommands.decrementAndGet();
            }
        }, null));
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

    /** 任务执行线程投递完成事件（不等待）。事件进入缓冲区，由工作线程按模拟时刻成批应用。 */
    private void emitFinish(FinishRecord rec) {
        long seq = eventSeq.incrementAndGet();
        events.add(new Event(seq, "finish " + rec.planId() + "/" + rec.taskId(),
                () -> {}, rec));
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

    /**
     * 一份计划按模拟时刻排序的执行时间线：每一步何时开始、落在哪台机器人、
     * 何时结束，以及中途的抢占/重试/重规划/追加事件。事件由单工作线程串行写入，
     * 相同输入序列重放两遍得到的时间线一致。
     */
    public List<TimelineEntry> timeline(String planId) {
        synchronized (lock) {
            if (!state.plans().containsKey(planId)) {
                throw new IllegalArgumentException("unknown plan: " + planId);
            }
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
                // 并行执行下，该机器人上至多有一个在跑任务；把它抢占回 PENDING（不计尝试）。
                for (RunningTask rt : new ArrayList<>(runningTasks.values())) {
                    if (robotId.equals(rt.runtime().assignedRobotId())) {
                        preemptRunning(rt, "robot " + robotId + " unavailable");
                    }
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
                RunningTask rt2 = runningTasks.get(Scheduler.key(planId, taskId));
                if (rt2 != null) {
                    preemptRunning(rt2, "task " + taskId + " cancelled");
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

    // ------------------------------------------------------------------
    // 运行中追加任务
    // ------------------------------------------------------------------

    /**
     * 向一个在跑计划追加一批新任务（整体成功或整体不做）。
     *
     * 校验顺序与可区分原因：
     * 1. 计划不存在或已到终态          -> INVALID_PLAN
     * 2. 批次为空 / 批内或与已有任务重号 -> INVALID_PLAN / DUPLICATE_TASK_ID
     * 3. 依赖了不存在的任务             -> MISSING_DEPENDENCY
     * 4. 与已有任务合成环               -> CYCLIC_DEPENDENCY
     * 5. 绑定了不存在的机器人           -> UNKNOWN_REQUIRED_ROBOT
     * 6. 合并后无可行调度               -> NO_CAPABLE_ROBOT / RESOURCE_CONFLICT
     *
     * 任一步失败即整批拒绝，原计划状态不被触碰，照常执行；已接受/已完成的步骤
     * 不会被修改或删除。追加成功的步骤与初始步骤遵守同一套依赖、超时重试、
     * 抢占与重规划规则。
     */
    public void appendTasks(String planId, List<TaskSpec> newTasks) {
        call("append " + planId, () -> {
            synchronized (lock) {
                Plan plan = state.plans().get(planId);
                if (plan == null) {
                    throw new PlanRejectedException(RejectionReason.INVALID_PLAN,
                            "unknown plan: " + planId);
                }
                if (isTerminal(planId)) {
                    throw new PlanRejectedException(RejectionReason.INVALID_PLAN,
                            "plan " + planId + " already terminal: "
                                    + state.planStatus(planId));
                }
                if (newTasks == null || newTasks.isEmpty()) {
                    throw new PlanRejectedException(RejectionReason.INVALID_PLAN,
                            "no tasks to append to plan " + planId);
                }

                // 合并后的候选计划：已有任务原样保留，新任务追加在后。
                Plan combined = plan.withAppendedTasks(newTasks);
                // 静态校验：重号（批内或与已有）、缺失依赖、环、未知绑定机器人。
                validator.validate(combined, state.robots());

                // 可行性：已有 RUNNING 工作形成固定占用，SUCCEEDED 工作保留，
                // 新任务按 PENDING 参与排程；排不出可行方案则整批拒绝。
                List<Plan> accepted = new ArrayList<>(state.plans().values());
                accepted.replaceAll(p -> p.planId().equals(planId) ? combined : p);
                Schedule schedule = scheduler.reschedule(combined, state.robots(),
                        accepted, state.flatRuntimes(), tick);
                if (!schedule.feasible()) {
                    LOG.info("append rejected planId={} reason={} newTasks={}",
                            planId, schedule.reason(),
                            newTasks.stream().map(TaskSpec::taskId).toList());
                    throw new PlanRejectedException(schedule.reason(),
                            "append to plan " + planId + " not feasible: "
                                    + schedule.reason());
                }

                // 全部校验通过才落状态：此前任何失败都不会改动在跑计划。
                state.appendTasks(combined, newTasks, tick);
                state.log(planId, tick, "APPEND",
                        "appended " + newTasks.size() + " task(s): "
                                + newTasks.stream().map(TaskSpec::taskId).toList());
                for (TaskSpec t : newTasks) {
                    state.recordTimeline(planId, tick, "APPEND", t.taskId(),
                            t.requiredRobotId(), "task appended to running plan");
                }
                for (TaskAssignment a : schedule.assignments()) {
                    if (newTasks.stream().anyMatch(t -> t.taskId().equals(a.taskId()))) {
                        state.log(planId, a.startTick(), "SCHEDULE",
                                "task " + a.taskId() + " -> robot " + a.robotId()
                                        + " [" + a.startTick() + "," + a.endTick() + ")");
                    }
                }
                LOG.info("append accepted planId={} newTasks={} tick={}", planId,
                        newTasks.stream().map(TaskSpec::taskId).toList(), tick);
            }
        });
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

                List<Plan> activePlans = activePlans();
                if (activePlans.isEmpty()) {
                    return;
                }

                // 为每个活动计划计算确定性调度，选取当前 tick 可启动的最高优先任务。
                // 已启动的任务成为 RUNNING 固定占用，下一轮重算自然错开同一台机器人，
                // 因此只要还有空闲机器人与就绪任务，就可以在同一 tick 并行下发多个任务。
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
        long finishTick = startTick + spec.durationTicks();
        Future<?> future = taskPool.submit(() -> {
            AttemptOutcome outcome;
            try {
                outcome = executionService.executeAttempt(spec, attemptNumber, startTick);
            } catch (Throwable t) {
                outcome = AttemptOutcome.FAILURE;
                LOG.error("execution threw for task {}", spec.taskId(), t);
            }
            emitFinish(new FinishRecord(d.planId(), d.taskId(), launchId, attemptNumber,
                    d.robotId(), startTick, finishTick, outcome));
        });
        runningTasks.put(Scheduler.key(d.planId(), d.taskId()),
                new RunningTask(d.planId(), spec, rt, future, startTick, finishTick, launchId));
        state.recordTimeline(d.planId(), startTick, "DISPATCH", d.taskId(), d.robotId(),
                "attempt " + attemptNumber + " started on robot " + d.robotId());
    }

    private void onAttemptFinished(FinishRecord rec) {
        synchronized (lock) {
            TaskRuntime rt = state.taskRuntimes(rec.planId()).get(rec.taskId());
            Plan plan = state.plans().get(rec.planId());
            TaskSpec spec = plan.task(rec.taskId());

            // 用唯一 launchId 判定陈旧：被抢占后同一任务可能已重新启动（新的 launchId），
            // 旧尝试的完成事件必须被忽略，不能影响当前状态。
            String runKey = Scheduler.key(rec.planId(), rec.taskId());
            RunningTask runningTask = runningTasks.get(runKey);
            boolean stale = runningTask == null
                    || runningTask.launchId() != rec.launchId();
            if (stale) {
                LOG.info("stale finish ignored plan={} task={} launchId={}",
                        rec.planId(), rec.taskId(), rec.launchId());
                return;
            }
            runningTasks.remove(runKey);

            long finishTick = rec.finishTick();
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
                        state.recordTimeline(rec.planId(), tick, "COMPLETE", rec.taskId(),
                                rec.robotId(), "succeeded after " + rec.attemptNumber()
                                        + " attempt(s)");
                        LOG.info("task succeeded plan={} task={} attempts={} tick={}",
                                rec.planId(), rec.taskId(), rec.attemptNumber(), tick);
                    }
                    case PREEMPTED -> {
                        // 通常抢占已在命令事件中复位；兜底处理（中断极快到达的情形）。
                        rt.markPreempted();
                        state.log(rec.planId(), tick, "PREEMPT",
                                "task " + rec.taskId() + " preempted, attempt "
                                        + rec.attemptNumber() + " not counted, will reassign");
                        state.recordTimeline(rec.planId(), tick, "PREEMPT", rec.taskId(),
                                rec.robotId(), "attempt " + rec.attemptNumber()
                                        + " not counted, will reassign");
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
                            state.recordTimeline(rec.planId(), tick, "EXHAUSTED", rec.taskId(),
                                    rec.robotId(), terminal + " after " + rec.attemptNumber()
                                            + " attempt(s)");
                            LOG.info("task exhausted plan={} task={} status={} tick={}",
                                    rec.planId(), rec.taskId(), terminal, tick);
                            cascadeDownstream(rec.planId());
                        } else {
                            rt.resetForRetry(rec.robotId());
                            state.log(rec.planId(), tick, "RETRY",
                                    "task " + rec.taskId() + " " + rec.outcome()
                                            + ", scheduling retry (" + rec.attemptNumber()
                                            + "/" + spec.maxAttempts() + " used)");
                            state.recordTimeline(rec.planId(), tick, "RETRY", rec.taskId(),
                                    rec.robotId(), rec.outcome() + ", scheduling retry ("
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

    private void preemptRunning(RunningTask victim, String reason) {
        state.log(victim.planId(), tick, "INTERRUPT",
                "interrupting task " + victim.spec().taskId() + ": " + reason);
        state.recordTimeline(victim.planId(), tick, "INTERRUPT", victim.spec().taskId(),
                victim.runtime().assignedRobotId(), reason);
        LOG.info("preempt running plan={} task={} reason={}",
                victim.planId(), victim.spec().taskId(), reason);
        victim.future().cancel(true);
        // 立即复位：释放机器人、回到 PENDING、被打断的尝试不计次数；
        // 之后该任务可被立刻重新分配。稍后到达的旧完成事件会因 launchId 不匹配而被忽略。
        victim.runtime().markPreempted();
        runningTasks.remove(Scheduler.key(victim.planId(), victim.spec().taskId()));
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
                        RunningTask victim = runningTasks.get(Scheduler.key(planId, t.taskId()));
                        if (rt.status() == TaskStatus.RUNNING && victim != null) {
                            preemptRunning(victim, "downstream of " + dep + " " + ds);
                        }
                        if (!rt.status().isTerminal()) {
                            rt.transition(TaskStatus.SKIPPED, null, rt.assignedRobotId(), tick);
                            state.log(planId, tick, "SKIP",
                                    "task " + t.taskId() + " skipped because dependency "
                                            + dep + " is " + ds);
                            state.recordTimeline(planId, tick, "SKIP", t.taskId(), null,
                                    "dependency " + dep + " is " + ds);
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
            state.recordTimeline(plan.planId(), tick, "REPLAN", null, null, trigger);
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
        state.recordTimeline(planId, tick, "PLAN-" + result, null, null,
                "plan converged to " + result);
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
