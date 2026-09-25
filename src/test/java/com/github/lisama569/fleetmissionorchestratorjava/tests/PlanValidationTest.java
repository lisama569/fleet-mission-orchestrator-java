package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 不可执行计划必须被拒绝，且失败原因可区分。
 */
class PlanValidationTest {

    private static final Logger LOG = LoggerFactory.getLogger(PlanValidationTest.class);

    private FleetOrchestrator orch;

    private static FleetOrchestrator newOrch() {
        FleetOrchestrator o = new FleetOrchestrator(List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "SCAN")), 1);
        o.start();
        return o;
    }

    @BeforeEach
    void setUp() { orch = newOrch(); }

    @AfterEach
    void tearDown() { orch.shutdown(); }

    private void assertRejected(Plan plan, RejectionReason expected, String inputDesc) {
        LOG.info("INPUT [{}] plan={} tasks={}", inputDesc, plan.planId(),
                plan.tasks().stream().map(t -> t.taskId() + t.dependencies()).toList());
        PlanRejectedException ex = org.junit.jupiter.api.Assertions.assertThrows(
                PlanRejectedException.class, () -> orch.submit(plan));
        LOG.info("REJECTED plan={} reason={} message={}", plan.planId(),
                ex.reason(), ex.getMessage());
        assertEquals(expected, ex.reason(),
                "expected " + expected + " but got " + ex.reason());
    }

    @Test
    void rejectsCyclicDependency() {
        Plan cyclic = Plan.of("CYCLE", 0,
                TaskSpec.simple("A", "MOVE", 1, List.of("B")),
                TaskSpec.simple("B", "MOVE", 1, List.of("A")));
        assertRejected(cyclic, RejectionReason.CYCLIC_DEPENDENCY, "cycle A<->B");
    }

    @Test
    void rejectsSelfCycle() {
        Plan self = Plan.of("SELF", 0,
                TaskSpec.simple("A", "MOVE", 1, List.of("A")));
        assertRejected(self, RejectionReason.CYCLIC_DEPENDENCY, "self cycle A->A");
    }

    @Test
    void rejectsMissingDependency() {
        Plan missing = Plan.of("MISSING", 0,
                TaskSpec.simple("A", "MOVE", 1, List.of("GHOST")));
        assertRejected(missing, RejectionReason.MISSING_DEPENDENCY, "dep on ghost");
    }

    @Test
    void rejectsDuplicateTaskId() {
        Plan dup = Plan.of("DUP", 0,
                TaskSpec.simple("A", "MOVE", 1, List.of()),
                TaskSpec.simple("A", "MOVE", 1, List.of()));
        assertRejected(dup, RejectionReason.DUPLICATE_TASK_ID, "duplicate task id");
    }

    @Test
    void rejectsDuplicatePlanId() {
        Plan p = Plan.of("SAMENAME", 0, TaskSpec.simple("A", "MOVE", 1, List.of()));
        orch.submit(p);
        Plan again = Plan.of("SAMENAME", 0, TaskSpec.simple("B", "MOVE", 1, List.of()));
        assertRejected(again, RejectionReason.DUPLICATE_PLAN_ID, "duplicate plan id");
    }

    @Test
    void rejectsUnknownRequiredRobot() {
        TaskSpec bound = TaskSpec.builder("A", "MOVE", 1).requireRobot("RX").build();
        assertRejected(Plan.of("UNK", 0, bound),
                RejectionReason.UNKNOWN_REQUIRED_ROBOT, "bind unknown robot");
    }

    @Test
    void rejectsNoCapableRobot() {
        // 车队中没有任何机器人能执行 CHARGE
        TaskSpec t = TaskSpec.simple("A", "CHARGE", 1, List.of());
        assertRejected(Plan.of("NOCAP", 0, t),
                RejectionReason.NO_CAPABLE_ROBOT, "no capable robot");
    }

    @Test
    void rejectsDeadlineResourceConflict() {
        // 唯一一台 R1，A 占 5 ticks，B 与 A 无依赖、必须在第 4 tick 前完成 -> 冲突
        Plan plan = Plan.of("DEADLINE", 0,
                TaskSpec.builder("A", "MOVE", 5).requireRobot("R1").build(),
                TaskSpec.builder("B", "MOVE", 3).requireRobot("R1").deadline(4).build());
        assertRejected(plan, RejectionReason.RESOURCE_CONFLICT, "deadline cannot be met");
    }

    @Test
    void rejectsEmptyPlan() {
        assertRejected(Plan.of("EMPTY", 0), RejectionReason.INVALID_PLAN, "empty plan");
    }
}
