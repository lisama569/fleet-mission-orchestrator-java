package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中动态重规划：
 *  - 机器人下线：在跑任务被抢占（尝试不计入），未完成部分重新分配到其它机器人，最终一致
 *  - 任务取消：已完成工作保留，下游 SKIPPED，取消收敛到确定终态
 *  - 高优先级插入：在跑的低优先级不被打断，新计划到达后结论确定
 *  - 相同输入序列重跑，最终状态可复现
 */
class DynamicReplanningTest {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicReplanningTest.class);
    private static final long MS_PER_TICK = 2;

    private List<Robot> fleet() {
        return List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
    }

    private TaskRuntimeView get(PlanSnapshot snap, String taskId) {
        return snap.tasks().stream().filter(t -> t.taskId().equals(taskId)).findFirst().orElseThrow();
    }

    /** 机器人下线：A 在某台机器人上跑到一半，该机器人下线，A 必须迁移到其它机器人完成。 */
    @Test
    void robotUnavailableReassignsRunningWork() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("ROBOT-DOWN", 0,
                    TaskSpec.builder("A", "MOVE", 100).build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build());
            orch.submit(plan);

            // 等待 A 确实开始执行，记录实际承载它的机器人，然后让该机器人下线
            String firstRobot = waitForAnyRunningRobot(orch, "ROBOT-DOWN", "A");
            LOG.info("INPUT event: robot {} becomes unavailable while A is running", firstRobot);
            orch.robotUnavailable(firstRobot);

            PlanSnapshot snap = orch.awaitPlan("ROBOT-DOWN");
            LOG.info(orch.explain("ROBOT-DOWN"));

            assertEquals(PlanStatus.COMPLETED, snap.status());
            var a = get(snap, "A");
            assertEquals(TaskStatus.SUCCEEDED, a.status());
            assertNotEquals(firstRobot, a.assignedRobotId(),
                    "A must finish on a different robot after migration");
            assertTrue(List.of("R1", "R2", "R3").contains(a.assignedRobotId()));
            assertEquals(1, a.attempts(), "preempted attempt must not be counted");
            var b = get(snap, "B");
            assertEquals(TaskStatus.SUCCEEDED, b.status());
            assertNotEquals(firstRobot, b.assignedRobotId());
        } finally {
            orch.shutdown();
        }
    }

    /** 任务取消：A 已完成的工作保留；B 取消后其下游 D 必须 SKIPPED；计划收敛为 FAILED。 */
    @Test
    void cancelTaskKeepsCompletedWorkAndSkipsDownstream() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("CANCEL", 0,
                    TaskSpec.builder("A", "MOVE", 5).build(),
                    TaskSpec.builder("B", "MOVE", 60).dependsOn("A").build(),
                    TaskSpec.builder("C", "MOVE", 2).dependsOn("A").build(),
                    TaskSpec.builder("D", "MOVE", 2).dependsOn("B", "C").build());
            orch.submit(plan);
            waitForStatus(orch, "CANCEL", "A",
                    TaskStatus.SUCCEEDED);
            // B 此时应在运行
            LOG.info("INPUT event: cancel B after A completed");
            orch.cancelTask("CANCEL", "B");

            PlanSnapshot snap = orch.awaitPlan("CANCEL");
            LOG.info(orch.explain("CANCEL"));

            var a = get(snap, "A");
            assertEquals(TaskStatus.SUCCEEDED,
                    a.status(), "completed work must be retained");
            var b = get(snap, "B");
            assertEquals(TaskStatus.CANCELLED,
                    b.status());
            var d = get(snap, "D");
            assertEquals(TaskStatus.SKIPPED,
                    d.status(), "downstream of cancelled task must be skipped");
            // C 不受 B 取消影响，可以完成
            var c = get(snap, "C");
            assertEquals(TaskStatus.SUCCEEDED,
                    c.status());
            assertEquals(PlanStatus.FAILED, snap.status());
        } finally {
            orch.shutdown();
        }
    }

    /** 取消已经完成的任务必须被忽略，计划照常 COMPLETED。 */
    @Test
    void cancellingCompletedTaskIsNoOp() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("CANCEL-DONE", 0,
                    TaskSpec.builder("A", "MOVE", 3).build());
            orch.submit(plan);
            orch.awaitPlan("CANCEL-DONE");
            orch.cancelTask("CANCEL-DONE", "A"); // 已完成，不能回退
            PlanSnapshot snap = orch.snapshot("CANCEL-DONE");
            assertEquals(PlanStatus.COMPLETED, snap.status());
            assertEquals(TaskStatus.SUCCEEDED,
                    get(snap, "A").status());
        } finally {
            orch.shutdown();
        }
    }

    /**
     * 高优先级插入：低优先级计划正在 R1 上执行长任务，此时插入同样需要 R1 的高优先级计划。
     * 运行中的任务不被打断；高优先级计划在 R1 释放后立即执行，两者最终都 COMPLETED。
     */
    @Test
    void highPriorityPlanWaitsForRunningButGetsResourceNext() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan low = Plan.of("LOW", 0,
                    TaskSpec.builder("L1", "MOVE", 40).requireRobot("R1").priority(1).build(),
                    TaskSpec.builder("L2", "MOVE", 2).requireRobot("R1")
                            .priority(1).dependsOn("L1").build());
            orch.submit(low);
            waitForRunning(orch, "LOW", "L1", "R1");

            Plan high = Plan.of("HIGH", 0,
                    TaskSpec.builder("H1", "MOVE", 2).requireRobot("R1").priority(99).build());
            LOG.info("INPUT event: insert HIGH priority plan requiring R1");
            orch.insertHighPriority(high, 0);

            PlanSnapshot lowSnap = orch.awaitPlan("LOW");
            PlanSnapshot highSnap = orch.awaitPlan("HIGH");
            LOG.info(orch.explain("LOW"));
            LOG.info(orch.explain("HIGH"));

            assertEquals(PlanStatus.COMPLETED, lowSnap.status());
            assertEquals(PlanStatus.COMPLETED, highSnap.status());
            long hStart = highSnap.tasks().get(0).startTick();
            long l1End = get(lowSnap, "L1").endTick();
            LOG.info("DECISION highStartTick={} lowL1EndTick={}", hStart, l1End);
            assertTrue(hStart >= l1End,
                    "high priority must not interrupt running task; starts only after R1 frees");
        } finally {
            orch.shutdown();
        }
    }

    /**
     * 机器人下线后唯一剩下的机器人也不能完成绑定任务时，计划必须以可区分原因失败，
     * 而不是悬挂。
     */
    @Test
    void replanningToNowhereFailsPlanDeterministically() {
        // 只有一台能干活的机器人（另两台能力不符）
        FleetOrchestrator orch = new FleetOrchestrator(
                List.of(Robot.online("R1", "MOVE"),
                        Robot.online("R2", "SCAN"),
                        Robot.online("R3", "SCAN")), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("NO-WAY-OUT", 0,
                    TaskSpec.builder("A", "MOVE", 80).requireRobot("R1").build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build());
            orch.submit(plan);
            waitForRunning(orch, "NO-WAY-OUT", "A", "R1");
            orch.robotUnavailable("R1");

            PlanSnapshot snap = orch.awaitPlan("NO-WAY-OUT");
            LOG.info(orch.explain("NO-WAY-OUT"));
            assertEquals(PlanStatus.FAILED, snap.status());
            // 决策日志必须给出可解释的 INFEASIBLE 原因
            boolean hasReason = snap.decisions().stream()
                    .anyMatch(d -> d.category().equals("INFEASIBLE"));
            assertTrue(hasReason, "final state must be explainable via INFEASIBLE decision");
        } finally {
            orch.shutdown();
        }
    }

    /**
     * 相同输入序列（提交 -> 等 A 运行 -> 承载机器人下线）执行两次，
     * 重规划后的最终任务状态/承载机器人/尝试次数必须完全一致。
     */
    @Test
    void replanningOutcomeIsReproducibleForSameInputSequence() {
        String first = runRobotDownScenario();
        String second = runRobotDownScenario();
        LOG.info("REPLAN DETERMINISM identicalFinalState={}", first.equals(second));
        assertEquals(first, second,
                "replanning under the same input sequence must be reproducible");
    }

    private String runRobotDownScenario() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("REP", 0,
                    TaskSpec.builder("A", "MOVE", 100).build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build());
            orch.submit(plan);
            String robot = waitForAnyRunningRobot(orch, "REP", "A");
            orch.robotUnavailable(robot);
            PlanSnapshot snap = orch.awaitPlan("REP");
            StringBuilder sb = new StringBuilder("down=").append(robot).append(';');
            for (var t : snap.tasks()) {
                sb.append(t.taskId()).append(':').append(t.status())
                        .append('@').append(t.assignedRobotId())
                        .append("#").append(t.attempts()).append(';');
            }
            return sb.toString();
        } finally {
            orch.shutdown();
        }
    }

    // ---------------------------------------------------------------
    // 轮询辅助（只在测试线程中使用，不影响工作线程的确定性）
    // ---------------------------------------------------------------

    private void waitForRunning(FleetOrchestrator orch, String planId,
                                String taskId, String robotId) throws RuntimeException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            PlanSnapshot snap = orch.snapshot(planId);
            var t = snap.tasks().stream().filter(x -> x.taskId().equals(taskId)).findFirst();
            if (t.isPresent() && t.get().status()
                    == TaskStatus.RUNNING
                    && robotId.equals(t.get().assignedRobotId())) {
                return;
            }
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new IllegalStateException("task " + taskId + " never became RUNNING on " + robotId);
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
}
