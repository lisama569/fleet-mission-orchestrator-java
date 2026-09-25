package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行过程中的失败、超时与重试：
 *  - 瞬时失败在重试后成功，已完成步骤只生效一次
 *  - 超时后重试成功
 *  - 持续失败耗尽重试 -> FAILED，其下游 SKIPPED，计划 FAILED
 */
class ExecutionRetryTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionRetryTest.class);
    private static final long MS_PER_TICK = 5;

    private FleetOrchestrator orch;

    @BeforeEach
    void setUp() {
        orch = new FleetOrchestrator(List.of(
                Robot.online("R1", "MOVE", "SCAN"),
                Robot.online("R2", "MOVE")), MS_PER_TICK);
        orch.start();
    }

    @AfterEach
    void tearDown() { orch.shutdown(); }

    @Test
    void flakyFailureRetriesAndSucceeds() {
        Plan plan = Plan.of("FLAKY", 0,
                TaskSpec.builder("A", "MOVE", 2)
                        .behavior(TaskBehavior.FAIL_THEN_SUCCEED).flakyAttempts(1)
                        .maxAttempts(3).build(),
                TaskSpec.builder("B", "SCAN", 1).dependsOn("A").build());
        LOG.info("INPUT flaky plan: A fails once then succeeds, B depends on A");
        PlanSnapshot snap = orch.submitAndWait(plan);
        LOG.info(orch.explain("FLAKY"));

        assertEquals(PlanStatus.COMPLETED, snap.status());
        var a = snap.tasks().stream().filter(t -> t.taskId().equals("A")).findFirst().orElseThrow();
        assertEquals(TaskStatus.SUCCEEDED, a.status());
        assertEquals(2, a.attempts(), "A should succeed on the 2nd attempt");
        var b = snap.tasks().stream().filter(t -> t.taskId().equals("B")).findFirst().orElseThrow();
        assertEquals(TaskStatus.SUCCEEDED, b.status());
        assertEquals(1, b.attempts(), "completed dependency must not be re-executed");
    }

    @Test
    void timeoutRetriesAndSucceeds() {
        // 行为模型：第 1 次尝试超时，第 2 次正常成功；同时给足 maxAttempts。
        Plan plan = Plan.of("TIMEOUT", 0,
                TaskSpec.builder("A", "MOVE", 2)
                        .behavior(TaskBehavior.TIMEOUT_THEN_SUCCEED).flakyAttempts(1)
                        .maxAttempts(2).build());
        LOG.info("INPUT timeout plan: A attempt1 TIMEOUT, attempt2 SUCCESS");
        PlanSnapshot snap = orch.submitAndWait(plan);
        LOG.info(orch.explain("TIMEOUT"));

        assertEquals(PlanStatus.COMPLETED, snap.status());
        var a = snap.tasks().stream().filter(t -> t.taskId().equals("A")).findFirst().orElseThrow();
        assertEquals(TaskStatus.SUCCEEDED, a.status());
        assertEquals(2, a.attempts());
    }

    @Test
    void alwaysFailingTaskExhaustsRetriesAndSkipsDownstream() {
        Plan plan = Plan.of("FATAL", 0,
                TaskSpec.builder("A", "MOVE", 1)
                        .behavior(TaskBehavior.FAIL_ALWAYS).maxAttempts(2).build(),
                TaskSpec.builder("B", "MOVE", 1).dependsOn("A").build(),
                TaskSpec.builder("C", "MOVE", 1).dependsOn("B").build());
        LOG.info("INPUT fatal plan: A always fails x2, B/C must SKIP");
        PlanSnapshot snap = orch.submitAndWait(plan);
        LOG.info(orch.explain("FATAL"));

        assertEquals(PlanStatus.FAILED, snap.status());
        var a = snap.tasks().stream().filter(t -> t.taskId().equals("A")).findFirst().orElseThrow();
        var b = snap.tasks().stream().filter(t -> t.taskId().equals("B")).findFirst().orElseThrow();
        var c = snap.tasks().stream().filter(t -> t.taskId().equals("C")).findFirst().orElseThrow();
        assertEquals(TaskStatus.FAILED, a.status());
        assertEquals(2, a.attempts(), "exactly maxAttempts tries, no more");
        assertEquals(TaskStatus.SKIPPED, b.status());
        assertEquals(TaskStatus.SKIPPED, c.status());
        assertEquals(0, b.attempts(), "skipped task never executed");
    }

    @Test
    void timeoutWithoutRetriesConvergesToTimedOut() {
        Plan plan = Plan.of("HARD-TO", 0,
                TaskSpec.builder("A", "MOVE", 5)
                        .timeout(2L).maxAttempts(1).build());
        LOG.info("INPUT hard timeout: A times out with no retries allowed");
        PlanSnapshot snap = orch.submitAndWait(plan);
        LOG.info(orch.explain("HARD-TO"));

        assertEquals(PlanStatus.FAILED, snap.status());
        var a = snap.tasks().stream().filter(t -> t.taskId().equals("A")).findFirst().orElseThrow();
        assertEquals(TaskStatus.TIMED_OUT, a.status());
        assertTrue(a.endTick() > a.startTick());
    }
}
