package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntimeView;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中追加任务：
 *  - 追加成功：新步骤可依赖已有步骤，后续追加可依赖先追加的步骤
 *  - 整批拒绝：重号/缺失依赖/成环/未知机器人/无可行方案，原因可区分
 *  - 被拒绝时原计划照常跑完，已完成工作一步不丢
 *  - 追加的步骤遵守与初始步骤相同的依赖、重试、抢占与重规划规则
 */
class PlanAppendTest {

    private static final Logger LOG = LoggerFactory.getLogger(PlanAppendTest.class);
    private static final long MS_PER_TICK = 2;

    private List<Robot> fleet() {
        return List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
    }

    private TaskRuntimeView get(PlanSnapshot snap, String taskId) {
        return snap.tasks().stream().filter(t -> t.taskId().equals(taskId))
                .findFirst().orElseThrow();
    }

    /** 追加成功：C 依赖已有的 A，D 依赖同批追加的 C；之后再次追加的 E 依赖上一批的 D。 */
    @Test
    void appendWhileRunningExtendsPlanAndChainsAcrossBatches() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("APP", 0,
                    TaskSpec.builder("A", "MOVE", 40).build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build()));
            waitForStatus(orch, "APP", "A", TaskStatus.RUNNING);
            LOG.info("INPUT append batch1: C(dep A), D(dep C)");
            orch.appendTasks("APP", List.of(
                    TaskSpec.builder("C", "MOVE", 2).dependsOn("A").build(),
                    TaskSpec.builder("D", "MOVE", 2).dependsOn("C").build()));
            LOG.info("INPUT append batch2: E(dep D from batch1)");
            orch.appendTasks("APP", List.of(
                    TaskSpec.builder("E", "MOVE", 2).dependsOn("D").build()));

            PlanSnapshot snap = orch.awaitPlan("APP");
            LOG.info(orch.explain("APP"));
            assertEquals(PlanStatus.COMPLETED, snap.status());
            for (String id : List.of("A", "B", "C", "D", "E")) {
                assertEquals(TaskStatus.SUCCEEDED, get(snap, id).status(),
                        "appended task " + id + " must execute to success");
            }
            // 依赖顺序：C 在 A 之后开始，D 在 C 之后，E 在 D 之后
            assertTrue(get(snap, "C").startTick() >= get(snap, "A").endTick());
            assertTrue(get(snap, "D").startTick() >= get(snap, "C").endTick());
            assertTrue(get(snap, "E").startTick() >= get(snap, "D").endTick());
        } finally {
            orch.shutdown();
        }
    }

    /** 批内重号与和已有步骤重号都要以 DUPLICATE_TASK_ID 整批拒绝。 */
    @Test
    void appendDuplicateTaskIdRejected() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("DUP", 0,
                    TaskSpec.builder("A", "MOVE", 30).build()));
            waitForStatus(orch, "DUP", "A", TaskStatus.RUNNING);

            PlanRejectedException withinBatch = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("DUP", List.of(
                            TaskSpec.builder("X", "MOVE", 1).build(),
                            TaskSpec.builder("X", "MOVE", 1).build())));
            assertEquals(RejectionReason.DUPLICATE_TASK_ID, withinBatch.reason());
            LOG.info("REJECTED within-batch duplicate reason={}", withinBatch.reason());

            PlanRejectedException vsExisting = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("DUP", List.of(
                            TaskSpec.builder("A", "MOVE", 1).build())));
            assertEquals(RejectionReason.DUPLICATE_TASK_ID, vsExisting.reason());
            LOG.info("REJECTED duplicate-vs-existing reason={}", vsExisting.reason());

            PlanSnapshot snap = orch.awaitPlan("DUP");
            assertEquals(PlanStatus.COMPLETED, snap.status());
            assertEquals(1, snap.tasks().size(), "rejected batches must leave no residue");
        } finally {
            orch.shutdown();
        }
    }

    /** 缺失依赖、互相成环、绑定不存在的机器人：三种原因必须可区分。 */
    @Test
    void appendMissingDependencyCycleAndUnknownRobotRejectedWithDistinctReasons() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("BAD", 0,
                    TaskSpec.builder("A", "MOVE", 30).build()));
            waitForStatus(orch, "BAD", "A", TaskStatus.RUNNING);

            PlanRejectedException missing = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("BAD", List.of(
                            TaskSpec.builder("Z", "MOVE", 1).dependsOn("NOPE").build())));
            assertEquals(RejectionReason.MISSING_DEPENDENCY, missing.reason());

            PlanRejectedException cycle = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("BAD", List.of(
                            TaskSpec.builder("X", "MOVE", 1).dependsOn("Y").build(),
                            TaskSpec.builder("Y", "MOVE", 1).dependsOn("X").build())));
            assertEquals(RejectionReason.CYCLIC_DEPENDENCY, cycle.reason());

            PlanRejectedException unknownRobot = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("BAD", List.of(
                            TaskSpec.builder("W", "MOVE", 1).requireRobot("RX").build())));
            assertEquals(RejectionReason.UNKNOWN_REQUIRED_ROBOT, unknownRobot.reason());
            LOG.info("REJECTED missing={} cycle={} unknownRobot={}",
                    missing.reason(), cycle.reason(), unknownRobot.reason());

            PlanSnapshot snap = orch.awaitPlan("BAD");
            assertEquals(PlanStatus.COMPLETED, snap.status());
            assertEquals(1, snap.tasks().size());
        } finally {
            orch.shutdown();
        }
    }

    /** 排不出可行方案（无能力机器人 / 截止冲突）时整批拒绝，原因可区分。 */
    @Test
    void appendInfeasibleRejected() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("INF", 0,
                    TaskSpec.builder("A", "MOVE", 30).build()));
            waitForStatus(orch, "INF", "A", TaskStatus.RUNNING);

            PlanRejectedException noCapable = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("INF", List.of(
                            TaskSpec.builder("V", "FLY", 1).build())));
            assertEquals(RejectionReason.NO_CAPABLE_ROBOT, noCapable.reason());

            PlanRejectedException conflict = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("INF", List.of(
                            TaskSpec.builder("U", "MOVE", 50).deadline(1L).build())));
            assertEquals(RejectionReason.RESOURCE_CONFLICT, conflict.reason());
            LOG.info("REJECTED noCapable={} conflict={}",
                    noCapable.reason(), conflict.reason());

            PlanSnapshot snap = orch.awaitPlan("INF");
            assertEquals(PlanStatus.COMPLETED, snap.status());
            assertEquals(1, snap.tasks().size());
        } finally {
            orch.shutdown();
        }
    }

    /** 已终结的计划不允许再追加。 */
    @Test
    void appendToTerminalPlanRejected() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("DONE", 0,
                    TaskSpec.builder("A", "MOVE", 2).build()));
            PlanSnapshot done = orch.awaitPlan("DONE");
            assertEquals(PlanStatus.COMPLETED, done.status());

            PlanRejectedException rejected = assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("DONE", List.of(
                            TaskSpec.builder("B", "MOVE", 1).build())));
            assertEquals(RejectionReason.INVALID_PLAN, rejected.reason());
            LOG.info("REJECTED append to terminal plan reason={}", rejected.reason());
            assertEquals(1, orch.snapshot("DONE").tasks().size());
        } finally {
            orch.shutdown();
        }
    }

    /**
     * 追加被拒绝时在跑计划不受影响：A 跑到一半时追加一批非法任务，
     * 原计划照常 COMPLETED，已完成的 A 保持 SUCCEEDED、尝试次数不变。
     */
    @Test
    void rejectedAppendLeavesRunningPlanUntouched() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("KEEP", 0,
                    TaskSpec.builder("A", "MOVE", 10).build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build()));
            waitForStatus(orch, "KEEP", "A", TaskStatus.SUCCEEDED);
            waitForStatus(orch, "KEEP", "B", TaskStatus.RUNNING);
            LOG.info("INPUT append invalid batch while B is running");
            assertThrows(PlanRejectedException.class,
                    () -> orch.appendTasks("KEEP", List.of(
                            TaskSpec.builder("X", "MOVE", 1).dependsOn("Y").build(),
                            TaskSpec.builder("Y", "MOVE", 1).dependsOn("X").build())));

            PlanSnapshot snap = orch.awaitPlan("KEEP");
            LOG.info(orch.explain("KEEP"));
            assertEquals(PlanStatus.COMPLETED, snap.status());
            assertEquals(2, snap.tasks().size(), "no residue from rejected batch");
            assertEquals(TaskStatus.SUCCEEDED, get(snap, "A").status());
            assertEquals(1, get(snap, "A").attempts(), "completed work must not be re-run");
            assertEquals(TaskStatus.SUCCEEDED, get(snap, "B").status());
        } finally {
            orch.shutdown();
        }
    }

    /** 追加的步骤遵守同样的重试规则：第一次失败、第二次成功。 */
    @Test
    void appendedTaskFollowsSameRetryRules() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("RETRY-APP", 0,
                    TaskSpec.builder("A", "MOVE", 20).build()));
            waitForStatus(orch, "RETRY-APP", "A", TaskStatus.RUNNING);
            orch.appendTasks("RETRY-APP", List.of(
                    TaskSpec.builder("F", "MOVE", 2).dependsOn("A")
                            .behavior(TaskBehavior.FAIL_THEN_SUCCEED)
                            .flakyAttempts(1).maxAttempts(2).build()));

            PlanSnapshot snap = orch.awaitPlan("RETRY-APP");
            LOG.info(orch.explain("RETRY-APP"));
            assertEquals(PlanStatus.COMPLETED, snap.status());
            var f = get(snap, "F");
            assertEquals(TaskStatus.SUCCEEDED, f.status());
            assertEquals(2, f.attempts(), "appended task must retry like any initial task");
        } finally {
            orch.shutdown();
        }
    }

    /** 追加的步骤遵守同样的抢占/重规划规则：承载机器人下线后迁移到其它机器人完成。 */
    @Test
    void appendedTaskMigratesWhenRobotGoesDown() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("MIG", 0,
                    TaskSpec.builder("A", "MOVE", 30).build()));
            waitForStatus(orch, "MIG", "A", TaskStatus.RUNNING);
            orch.appendTasks("MIG", List.of(
                    TaskSpec.builder("L", "MOVE", 100).dependsOn("A").build()));
            String robot = waitForAnyRunningRobot(orch, "MIG", "L");
            LOG.info("INPUT event: robot {} down while appended task L running", robot);
            orch.robotUnavailable(robot);

            PlanSnapshot snap = orch.awaitPlan("MIG");
            LOG.info(orch.explain("MIG"));
            assertEquals(PlanStatus.COMPLETED, snap.status());
            var l = get(snap, "L");
            assertEquals(TaskStatus.SUCCEEDED, l.status());
            assertNotEquals(robot, l.assignedRobotId());
            assertEquals(1, l.attempts(), "preempted attempt must not be counted");
        } finally {
            orch.shutdown();
        }
    }

    // ---------------------------------------------------------------
    // 轮询辅助
    // ---------------------------------------------------------------

    private void waitForStatus(FleetOrchestrator orch, String planId, String taskId,
                               TaskStatus expected) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            PlanSnapshot snap = orch.snapshot(planId);
            var t = snap.tasks().stream().filter(x -> x.taskId().equals(taskId)).findFirst();
            if (t.isPresent() && t.get().status() == expected) {
                return;
            }
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new IllegalStateException("task " + taskId + " never reached " + expected);
    }

    private String waitForAnyRunningRobot(FleetOrchestrator orch, String planId, String taskId) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            PlanSnapshot snap = orch.snapshot(planId);
            var t = snap.tasks().stream().filter(x -> x.taskId().equals(taskId)).findFirst();
            if (t.isPresent() && t.get().status() == TaskStatus.RUNNING
                    && t.get().assignedRobotId() != null) {
                return t.get().assignedRobotId();
            }
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new IllegalStateException("task " + taskId + " never became RUNNING");
    }
}
