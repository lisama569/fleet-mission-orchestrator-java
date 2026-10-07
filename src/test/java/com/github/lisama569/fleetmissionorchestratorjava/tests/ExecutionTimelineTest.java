package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEntry;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行时间线：
 *  - 能看清每一步何时开始、落在哪台机器人、何时结束，以及重试/抢占/重规划事件
 *  - 事件按模拟时刻排序
 *  - 相同输入序列重放两遍，时间线逐条一致
 */
class ExecutionTimelineTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionTimelineTest.class);
    private static final long MS_PER_TICK = 2;

    private List<Robot> fleet() {
        return List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
    }

    private void logTimeline(String planId, List<TimelineEntry> timeline) {
        StringBuilder sb = new StringBuilder("\n===== TIMELINE plan=" + planId + " =====\n");
        for (TimelineEntry e : timeline) {
            sb.append(String.format("  tick=%-4d [%-14s] task=%-4s robot=%-4s %s%n",
                    e.tick(), e.eventType(), e.taskId(), e.robotId(), e.detail()));
        }
        LOG.info("{}", sb);
    }

    /** 带一次失败重试的计划：时间线必须按序呈现 DISPATCH -> RETRY -> DISPATCH -> COMPLETE。 */
    @Test
    void timelineShowsDispatchRetryAndCompletionInOrder() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("TL", 0,
                    TaskSpec.builder("A", "MOVE", 4)
                            .behavior(TaskBehavior.FAIL_THEN_SUCCEED)
                            .flakyAttempts(1).maxAttempts(2).build(),
                    TaskSpec.builder("B", "MOVE", 2).dependsOn("A").build());
            LOG.info("INPUT plan: A fails once then succeeds, B depends on A");
            PlanSnapshot snap = orch.submitAndWait(plan);
            assertEquals(PlanStatus.COMPLETED, snap.status());

            List<TimelineEntry> timeline = orch.timeline("TL");
            logTimeline("TL", timeline);

            // 按 tick 非递减排序
            for (int i = 1; i < timeline.size(); i++) {
                assertTrue(timeline.get(i).tick() >= timeline.get(i - 1).tick(),
                        "timeline must be ordered by tick");
            }
            // A 的完整生命周期：两次 DISPATCH、一次 RETRY、最终 COMPLETE，都带机器人
            List<TimelineEntry> aEvents = timeline.stream()
                    .filter(e -> "A".equals(e.taskId())).toList();
            assertEquals(2, aEvents.stream()
                    .filter(e -> e.eventType().equals("DISPATCH")).count());
            assertEquals(1, aEvents.stream()
                    .filter(e -> e.eventType().equals("RETRY")).count());
            assertEquals(1, aEvents.stream()
                    .filter(e -> e.eventType().equals("COMPLETE")).count());
            TimelineEntry aDone = aEvents.stream()
                    .filter(e -> e.eventType().equals("COMPLETE")).findFirst().orElseThrow();
            assertNotNull(aDone.robotId(), "completion must record the executing robot");
            // B 在 A 完成后才被派发
            TimelineEntry bDispatch = timeline.stream()
                    .filter(e -> "B".equals(e.taskId()) && e.eventType().equals("DISPATCH"))
                    .findFirst().orElseThrow();
            assertTrue(bDispatch.tick() >= aDone.tick(),
                    "B must be dispatched after A completed");
            // 计划级收尾事件
            assertTrue(timeline.stream().anyMatch(e -> e.eventType().equals("PLAN-COMPLETED")));
        } finally {
            orch.shutdown();
        }
    }

    /** 机器人下线触发抢占与重规划：时间线上能看到 INTERRUPT / REPLAN / 重新 DISPATCH。 */
    @Test
    void timelineShowsPreemptionAndReplanning() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("TL2", 0,
                    TaskSpec.builder("A", "MOVE", 100).build()));
            String robot = waitForAnyRunningRobot(orch, "TL2", "A");
            LOG.info("INPUT event: robot {} down while A running", robot);
            orch.robotUnavailable(robot);
            PlanSnapshot snap = orch.awaitPlan("TL2");
            assertEquals(PlanStatus.COMPLETED, snap.status());

            List<TimelineEntry> timeline = orch.timeline("TL2");
            logTimeline("TL2", timeline);
            assertTrue(timeline.stream().anyMatch(e -> e.eventType().equals("INTERRUPT")
                    && "A".equals(e.taskId())), "preemption must appear on timeline");
            assertTrue(timeline.stream().anyMatch(e -> e.eventType().equals("REPLAN")),
                    "replanning must appear on timeline");
            // 被抢占后 A 被重新派发到另一台机器人
            List<TimelineEntry> dispatches = timeline.stream()
                    .filter(e -> e.eventType().equals("DISPATCH") && "A".equals(e.taskId()))
                    .toList();
            assertEquals(2, dispatches.size());
            assertNotNull(dispatches.get(1).robotId());
            assertTrue(!dispatches.get(0).robotId().equals(dispatches.get(1).robotId()),
                    "A must be re-dispatched to a different robot");
        } finally {
            orch.shutdown();
        }
    }

    /** 追加动作也要上时间线，且同一输入序列重放两遍时间线逐条一致。 */
    @Test
    void timelineIsReproducibleAcrossReplays() {
        String first = runTimelineScenario();
        String second = runTimelineScenario();
        LOG.info("TIMELINE DETERMINISM identical={}", first.equals(second));
        assertEquals(first, second,
                "same input sequence must replay to an identical timeline");
    }

    private String runTimelineScenario() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            orch.submit(Plan.of("TL3", 0,
                    TaskSpec.builder("A", "MOVE", 6).build(),
                    TaskSpec.builder("B", "MOVE", 4).build()));
            waitForStatus(orch, "TL3", "A", TaskStatus.RUNNING);
            orch.appendTasks("TL3", List.of(
                    TaskSpec.builder("C", "MOVE", 2).dependsOn("A").build()));
            PlanSnapshot snap = orch.awaitPlan("TL3");
            assertEquals(PlanStatus.COMPLETED, snap.status());
            List<TimelineEntry> timeline = orch.timeline("TL3");
            logTimeline("TL3", timeline);
            // 追加事件可见
            assertTrue(timeline.stream().anyMatch(e -> e.eventType().equals("APPEND")
                    && "C".equals(e.taskId())));
            StringBuilder sb = new StringBuilder();
            for (TimelineEntry e : timeline) {
                sb.append(e.tick()).append('|').append(e.eventType()).append('|')
                        .append(e.taskId()).append('|').append(e.robotId()).append('|')
                        .append(e.detail()).append(';');
            }
            return sb.toString();
        } finally {
            orch.shutdown();
        }
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
