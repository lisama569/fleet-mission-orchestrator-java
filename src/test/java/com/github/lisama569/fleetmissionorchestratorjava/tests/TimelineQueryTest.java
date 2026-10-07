package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineKind;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行时间线：
 *  - 每一步何时、由哪台机器人执行、何时结束都可按时间先后查到
 *  - 抢占、重试、重规划、追加都体现在时间线上
 *  - 相同输入序列重放两遍，时间线逐字节一致
 */
class TimelineQueryTest {

    private static final Logger LOG = LoggerFactory.getLogger(TimelineQueryTest.class);
    private static final long MS_PER_TICK = 2;

    private List<Robot> fleet() {
        return List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
    }

    private String waitForRunningRobot(FleetOrchestrator o, String plan, String task) {
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            var t = o.snapshot(plan).tasks().stream()
                    .filter(x -> x.taskId().equals(task)).findFirst();
            if (t.isPresent() && t.get().status()
                    == com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus.RUNNING
                    && t.get().assignedRobotId() != null) {
                return t.get().assignedRobotId();
            }
            try { Thread.sleep(3); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new IllegalStateException(task + " never ran");
    }

    /** 时间线包含派发/完成、抢占、重规划事件，且按 tick 递增排列。 */
    @Test
    void timelineRecordsDispatchPreemptReplanAndCompletion() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("TL", 0,
                    TaskSpec.builder("A", "MOVE", 100).build(),
                    TaskSpec.builder("B", "MOVE", 5).dependsOn("A").build()));
            String robot = waitForRunningRobot(orch, "TL", "A");
            LOG.info("INPUT robot {} unavailable while A running", robot);
            orch.robotUnavailable(robot);

            PlanSnapshot snap = orch.awaitPlan("TL");
            assertEquals(PlanStatus.COMPLETED, snap.status());

            List<TimelineEvent> tl = orch.timeline("TL");
            LOG.info("TIMELINE:\n{}", describe(tl));
            assertTrue(contains(tl, TimelineKind.DISPATCH, "A"), "must show A dispatch");
            assertTrue(contains(tl, TimelineKind.PREEMPT, "A"), "must show A preempted");
            assertTrue(contains(tl, TimelineKind.REPLAN, ""), "must show replan trigger");
            assertTrue(contains(tl, TimelineKind.COMPLETE, "A"), "must show A completion");
            assertTrue(contains(tl, TimelineKind.COMPLETE, "B"), "must show B completion");

            // 时间线必须按 tick（及同 tick 内种类顺序）非递减排列
            for (int i = 1; i < tl.size(); i++) {
                long t0 = tl.get(i - 1).tick();
                long t1 = tl.get(i).tick();
                assertTrue(t0 <= t1, "timeline must be chronological");
            }
        } finally {
            orch.shutdown();
        }
    }

    /** 失败后重试：时间线应出现 RETRY，且同一任务出现两次 DISPATCH（不同 attempt）。 */
    @Test
    void timelineRecordsRetry() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("TLR", 0,
                    TaskSpec.builder("A", "MOVE", 2)
                            .behavior(TaskBehavior.FAIL_THEN_SUCCEED).flakyAttempts(1)
                            .maxAttempts(2).build()));
            orch.awaitPlan("TLR");
            List<TimelineEvent> tl = orch.timeline("TLR");
            LOG.info("RETRY TIMELINE:\n{}", describe(tl));
            assertTrue(contains(tl, TimelineKind.RETRY, "A"));
            long dispatches = tl.stream().filter(e -> e.kind() == TimelineKind.DISPATCH
                    && e.taskId().equals("A")).count();
            assertEquals(2, dispatches, "A must have two dispatch events (two attempts)");
            assertTrue(contains(tl, TimelineKind.COMPLETE, "A"));
        } finally {
            orch.shutdown();
        }
    }

    /** 时间线出现 APPEND 事件，且包含追加步骤的派发与完成。 */
    @Test
    void timelineRecordsAppend() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("TLA", 0,
                    TaskSpec.builder("A", "MOVE", 80).build()));
            waitForRunningRobot(orch, "TLA", "A");
            orch.appendTasks("TLA",
                    List.of(TaskSpec.builder("B", "MOVE", 10).dependsOn("A").build()));
            orch.awaitPlan("TLA");
            List<TimelineEvent> tl = orch.timeline("TLA");
            LOG.info("APPEND TIMELINE:\n{}", describe(tl));
            assertTrue(contains(tl, TimelineKind.APPEND, ""));
            assertTrue(contains(tl, TimelineKind.DISPATCH, "B"));
            assertTrue(contains(tl, TimelineKind.COMPLETE, "B"));
        } finally {
            orch.shutdown();
        }
    }

    /** 相同输入序列（提交->A 运行中让承载机器人下线）重放两遍，时间线完全一致。 */
    @Test
    void timelineIsReproducibleForSameInputSequence() {
        String first = runScenario();
        String second = runScenario();
        LOG.info("TIMELINE DETERMINISM identical={}", first.equals(second));
        assertEquals(first, second, "timeline must be reproducible for the same input sequence");
    }

    private String runScenario() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("DET", 0,
                    TaskSpec.builder("A", "MOVE", 90).build(),
                    TaskSpec.builder("B", "MOVE", 90).build(),
                    TaskSpec.builder("C", "MOVE", 8).dependsOn("A", "B").build()));
            String robot = waitForRunningRobot(orch, "DET", "A");
            orch.robotUnavailable(robot);
            orch.awaitPlan("DET");
            StringBuilder sb = new StringBuilder();
            for (TimelineEvent e : orch.timeline("DET")) {
                sb.append(e.tick()).append('|').append(e.kind()).append('|')
                        .append(e.taskId()).append('|').append(e.robotId())
                        .append('|').append(e.attempt()).append(';');
            }
            return sb.toString();
        } finally {
            orch.shutdown();
        }
    }

    private boolean contains(List<TimelineEvent> tl, TimelineKind kind, String taskId) {
        return tl.stream().anyMatch(e -> e.kind() == kind
                && (taskId.isEmpty() || e.taskId().equals(taskId)));
    }

    private String describe(List<TimelineEvent> tl) {
        StringBuilder sb = new StringBuilder();
        for (TimelineEvent e : tl) {
            sb.append(String.format("  tick=%-4d [%-9s] %-6s@%-3s attempt=%d %s%n",
                    e.tick(), e.kind(), e.taskId(), e.robotId(), e.attempt(), e.message()));
        }
        return sb.toString();
    }
}
