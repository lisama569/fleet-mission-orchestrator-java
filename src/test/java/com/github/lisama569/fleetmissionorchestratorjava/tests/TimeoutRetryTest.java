package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.api.StepSpec;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanState;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Step;
import com.github.lisama569.fleetmissionorchestratorjava.domain.StepState;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.ScheduleDecision;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败 / 超时重试、完成幂等与取消收敛：
 * 已完成步骤只生效一次；超时与取消都必须收敛到确定终态，继续 tick 不会复活。
 */
class TimeoutRetryTest {

    private static final Logger log = LoggerFactory.getLogger(TimeoutRetryTest.class);

    private ValidationRejectionTest.MutableClock clock;
    private Scheduler scheduler;

    private void init() {
        clock = new ValidationRejectionTest.MutableClock();
        scheduler = new Scheduler(clock);
        scheduler.registerRobot(new Robot("R1", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R2", Set.of("lift", "move")));
    }

    private StepSpec spec(String id, long duration, long timeout, int maxAttempts,
                          boolean alwaysFail, List<String> deps) {
        StepSpec s = new StepSpec();
        s.setId(id);
        s.setRequiredCapability("lift");
        s.setDurationMillis(duration);
        s.setTimeoutMillis(timeout);
        s.setMaxAttempts(maxAttempts);
        s.setAlwaysFail(alwaysFail);
        s.setDependencies(deps == null ? List.of() : deps);
        return s;
    }

    private PlanSubmission plan(String id, int priority, StepSpec... steps) {
        PlanSubmission p = new PlanSubmission();
        p.setId(id);
        p.setPriority(priority);
        p.setSteps(List.of(steps));
        return p;
    }

    private void tick(long delta) {
        clock.advance(delta);
        scheduler.tick();
    }

    private void runToQuiet() {
        int guard = 0;
        while (!scheduler.isQuiet()) {
            tick(5);
            if (++guard > 200_000) {
                throw new IllegalStateException("调度未收敛");
            }
        }
    }

    private long count(String action, String stepId) {
        return scheduler.decisions().stream()
                .filter(d -> d.action().equals(action)
                        && (stepId == null || stepId.equals(d.stepId())))
                .count();
    }

    @Test
    @DisplayName("失败重试：S1 先成功保留，S2 三次尝试后 FAILED_FINAL，下游 S3 级联取消")
    void failureRetryThenFinal() {
        init();
        scheduler.submit(plan("P-fail", 0,
                spec("S1", 20, 0, 1, false, List.of()),
                spec("S2", 10, 0, 3, true, List.of("S1")),
                spec("S3", 10, 0, 1, false, List.of("S2"))));

        log.info("[input] 计划 P-fail：S1 正常(20ms)，S2 alwaysFail maxAttempts=3，S3 依赖 S2");
        runToQuiet();

        Plan plan = scheduler.getPlan("P-fail");
        Step s1 = plan.getStep("S1");
        Step s2 = plan.getStep("S2");
        Step s3 = plan.getStep("S3");

        log.info("[decision] S2 决策={}", scheduler.decisions().stream()
                .filter(d -> "S2".equals(d.stepId()))
                .map(d -> d.action() + "(attempt=" + d.detail().replaceAll(".*attempt=", "") + ")")
                .toList());
        log.info("[final-state] plan={} S1={} S2={}(attempts={}) S3={}",
                plan.getState(), s1.getState(), s2.getState(), s2.getAttempt(), s3.getState());

        assertEquals(PlanState.FAILED_FINAL, plan.getState());
        assertEquals(StepState.SUCCEEDED, s1.getState(), "先完成的 S1 工作必须保留");
        assertEquals(StepState.FAILED, s2.getState());
        assertEquals(3, s2.getAttempt(), "必须恰好尝试 3 次");
        assertEquals(2, count("FAILED_RETRY", "S2"));
        assertEquals(1, count("FAILED_FINAL", "S2"));
        assertEquals(StepState.CANCELLED, s3.getState(), "下游未开始步骤级联取消");
        // 完成幂等：S1 的成功效果只生效一次
        assertEquals(1, count("SUCCEEDED", "S1"));
    }

    @Test
    @DisplayName("超时重试：timeout < duration，两次尝试后 TIMED_OUT 终态，计划 FAILED_FINAL")
    void timeoutRetryThenFinal() {
        init();
        scheduler.submit(plan("P-timeout", 0,
                spec("S1", 100, 20, 2, false, List.of())));

        log.info("[input] 计划 P-timeout：S1 duration=100ms timeout=20ms maxAttempts=2");
        runToQuiet();

        Plan plan = scheduler.getPlan("P-timeout");
        Step s1 = plan.getStep("S1");
        log.info("[decision] S1 决策序列={}", scheduler.decisions().stream()
                .filter(d -> "S1".equals(d.stepId()))
                .map(ScheduleDecision::action).toList());
        log.info("[final-state] plan={} S1={} attempts={}",
                plan.getState(), s1.getState(), s1.getAttempt());

        assertEquals(PlanState.FAILED_FINAL, plan.getState());
        assertEquals(StepState.TIMED_OUT, s1.getState());
        assertEquals(2, s1.getAttempt());
        assertEquals(1, count("TIMEOUT_RETRY", "S1"));
        assertEquals(1, count("TIMEOUT_FINAL", "S1"));
        assertEquals(0, count("SUCCEEDED", "S1"));
    }

    @Test
    @DisplayName("取消收敛：运行中取消，已完成步骤保留，未完成步骤 CANCELLED，继续 tick 不复活")
    void cancelMidRunConverges() {
        init();
        scheduler.submit(plan("P-cancel", 0,
                spec("S1", 20, 0, 1, false, List.of()),
                spec("S2", 100, 0, 1, false, List.of("S1"))));

        log.info("[input] 计划 P-cancel：S1(20ms) -> S2(100ms)，S1 成功后立即取消");
        tick(25); // S1 被分配并启动
        tick(25); // S1 到期成功保留，S2 紧接着被分配并运行
        scheduler.cancelPlan("P-cancel");

        Plan plan = scheduler.getPlan("P-cancel");
        log.info("[decision] 取消时 S1={} S2={}", plan.getStep("S1").getState(),
                plan.getStep("S2").getState());
        assertEquals(StepState.SUCCEEDED, plan.getStep("S1").getState(),
                "取消时已完成的 S1 必须保留");
        assertEquals(StepState.CANCELLED, plan.getStep("S2").getState());
        assertEquals(PlanState.CANCELLED, plan.getState());

        // 继续推进足够久：终态不得复活，机器人上不得再出现该计划的任务
        for (int i = 0; i < 50; i++) {
            tick(10);
        }
        plan = scheduler.getPlan("P-cancel");
        log.info("[final-state] 继续 tick 后 plan={} S1={} S2={}",
                plan.getState(), plan.getStep("S1").getState(), plan.getStep("S2").getState());
        assertEquals(PlanState.CANCELLED, plan.getState());
        assertEquals(StepState.SUCCEEDED, plan.getStep("S1").getState());
        assertEquals(StepState.CANCELLED, plan.getStep("S2").getState());
        assertTrue(scheduler.isQuiet());
    }

    @Test
    @DisplayName("Step 状态机：markSucceeded 重复调用不重复生效，终态不可回退")
    void successIsIdempotentAtDomainLevel() {
        Step step = new Step("X", "lift", null, 10, 0, 1, false, -1L, Long.MAX_VALUE, List.of());
        step.markAssigned("R1");
        step.markRunning();
        assertTrue(step.markSucceeded());
        assertFalse(step.markSucceeded(), "重复完成必须返回 false，效果不重复生效");
        assertFalse(step.cancel(), "SUCCEEDED 终态不可被取消覆盖");
        assertEquals(StepState.SUCCEEDED, step.getState());
        log.info("[final-state] 重复完成后 step={}", step);
    }
}
