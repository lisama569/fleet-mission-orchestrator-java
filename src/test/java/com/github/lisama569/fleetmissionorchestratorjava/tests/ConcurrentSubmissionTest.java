package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.api.StepSpec;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanState;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanValidationException;
import com.github.lisama569.fleetmissionorchestratorjava.domain.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.domain.StepState;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.ScheduleDecision;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发提交：同一批计划并发提交后，最终状态必须稳定一致；
 * 决策日志回放必须证明同一机器人同一时刻只被一个步骤占用。
 */
class ConcurrentSubmissionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmissionTest.class);

    private Scheduler newScheduler(ValidationRejectionTest.MutableClock clock) {
        Scheduler scheduler = new Scheduler(clock);
        scheduler.registerRobot(new Robot("R1", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R2", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R3", Set.of("crane")));
        return scheduler;
    }

    private StepSpec step(String id, String capability, long duration, List<String> deps) {
        StepSpec s = new StepSpec();
        s.setId(id);
        s.setRequiredCapability(capability);
        s.setDurationMillis(duration);
        s.setDependencies(new ArrayList<>(deps));
        return s;
    }

    private PlanSubmission plan(String id, int priority, StepSpec... steps) {
        PlanSubmission p = new PlanSubmission();
        p.setId(id);
        p.setPriority(priority);
        p.setSteps(List.of(steps));
        return p;
    }

    /** 确定性推进：手动时钟 + 单线程 tick，与墙钟无关。 */
    private void runToQuiet(Scheduler scheduler, ValidationRejectionTest.MutableClock clock) {
        int guard = 0;
        while (!scheduler.isQuiet()) {
            clock.advance(5);
            scheduler.tick();
            if (++guard > 200_000) {
                throw new IllegalStateException("调度未收敛");
            }
        }
    }

    /** 回放决策日志，验证资源独占：ASSIGN_START 时该机器人必须空闲。 */
    private void assertResourceExclusivity(List<ScheduleDecision> decisions) {
        Map<String, String> occupied = new HashMap<>();
        for (ScheduleDecision d : decisions) {
            switch (d.action()) {
                case "ASSIGN_START" -> {
                    String prev = occupied.put(d.robotId(), d.planId() + ":" + d.stepId());
                    assertTrue(prev == null,
                            "机器人 " + d.robotId() + " 被重复占用: " + prev + " 与 "
                                    + d.planId() + ":" + d.stepId());
                }
                case "SUCCEEDED", "FAILED_RETRY", "FAILED_FINAL",
                     "TIMEOUT_RETRY", "TIMEOUT_FINAL" -> occupied.remove(d.robotId());
                default -> { }
            }
        }
        assertTrue(occupied.isEmpty(), "收敛后仍有机器人被占用: " + occupied);
    }

    private Map<String, PlanState> snapshot(Scheduler scheduler) {
        Map<String, PlanState> states = new HashMap<>();
        for (Plan plan : scheduler.listPlans()) {
            states.put(plan.getId(), plan.getState());
        }
        return states;
    }

    @Test
    @DisplayName("20 个计划并发提交：全部收敛 COMPLETED，资源独占成立，两批运行结果一致")
    void concurrentDistinctPlans() throws InterruptedException {
        Map<String, PlanState> firstRun = runConcurrentBatch(1);
        Map<String, PlanState> secondRun = runConcurrentBatch(2);
        log.info("[final-state] 第一批={}", firstRun);
        log.info("[final-state] 第二批={}", secondRun);
        // 两批计划 id 不同，比较最终状态的多重集合（忽略 id）
        List<PlanState> firstStates = firstRun.values().stream().sorted().toList();
        List<PlanState> secondStates = secondRun.values().stream().sorted().toList();
        assertEquals(firstStates, secondStates, "相同输入的两批并发执行，最终状态必须一致");
        firstRun.forEach((id, state) -> assertEquals(PlanState.COMPLETED, state,
                "计划 " + id + " 必须收敛到 COMPLETED"));
    }

    private Map<String, PlanState> runConcurrentBatch(int batchNo) throws InterruptedException {
        ValidationRejectionTest.MutableClock clock = new ValidationRejectionTest.MutableClock();
        Scheduler scheduler = newScheduler(clock);

        int planCount = 20;
        // 池大小必须 >= 任务数：每个任务都会阻塞在 go 闩锁上等待同时放行
        ExecutorService pool = Executors.newFixedThreadPool(planCount);
        CountDownLatch ready = new CountDownLatch(planCount);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < planCount; i++) {
            final int idx = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    PlanSubmission p = plan("P" + batchNo + "-" + idx, idx % 3,
                            step("S1", "lift", 30, List.of()),
                            step("S2", "lift", 30, List.of("S1")));
                    scheduler.submit(p);
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("[input] 批次{} 并发提交 {} 个计划，提交异常数={}", batchNo, planCount, errors.size());
        assertTrue(errors.isEmpty(), "并发提交不应出现异常: " + errors);
        assertEquals(planCount, scheduler.listPlans().size());

        runToQuiet(scheduler, clock);

        List<ScheduleDecision> decisions = scheduler.decisions();
        log.info("[decision] 批次{} 决策数={} 分配决策={}", batchNo, decisions.size(),
                decisions.stream().filter(d -> d.action().equals("ASSIGN_START"))
                        .map(d -> d.planId() + ":" + d.stepId() + "->" + d.robotId()).toList());
        assertResourceExclusivity(decisions);

        for (Plan plan : scheduler.listPlans()) {
            for (var step : plan.getSteps()) {
                assertEquals(StepState.SUCCEEDED, step.getState(),
                        plan.getId() + "/" + step.getId() + " 必须成功");
            }
        }
        return snapshot(scheduler);
    }

    @Test
    @DisplayName("相同 plan id 并发提交：恰好一个成功，其余以 INVALID_PLAN 拒绝")
    void concurrentSamePlanId() throws InterruptedException {
        ValidationRejectionTest.MutableClock clock = new ValidationRejectionTest.MutableClock();
        Scheduler scheduler = newScheduler(clock);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> accepted = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<RejectionReason> rejected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    scheduler.submit(plan("P-race", 0, step("S1", "lift", 20, List.of())));
                    accepted.add("ok");
                } catch (PlanValidationException e) {
                    rejected.add(e.getReason());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("[input] {} 个线程并发提交同一 plan id=P-race", threads);
        log.info("[decision] 成功数={} 拒绝数={} 拒绝原因集合={}",
                accepted.size(), rejected.size(), new HashSet<>(rejected));
        assertEquals(1, accepted.size(), "同一 plan id 只能被接受一次");
        assertEquals(threads - 1, rejected.size());
        assertEquals(Set.of(RejectionReason.INVALID_PLAN), new HashSet<>(rejected));

        runToQuiet(scheduler, clock);
        Plan plan = scheduler.getPlan("P-race");
        log.info("[final-state] P-race 状态={} 步骤状态={}", plan.getState(),
                plan.getSteps().stream().map(s -> s.getId() + "=" + s.getState()).toList());
        assertEquals(PlanState.COMPLETED, plan.getState());
        assertEquals(1, scheduler.listPlans().size());
    }
}
