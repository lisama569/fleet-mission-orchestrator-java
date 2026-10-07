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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行执行：
 *  - 同一计划内彼此无依赖的步骤真的同时在多台机器人上执行（执行区间重叠、机器人不同）
 *  - 同一台机器人在同一时刻只干一件事（按机器人分组后区间不重叠）
 *  - 相同输入重跑两遍，每步落在哪台机器人、跑几次、什么状态、起止时刻完全一致
 */
class ParallelExecutionTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelExecutionTest.class);
    private static final long MS_PER_TICK = 3;

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

    /** 三个互不依赖的步骤必须在同一时刻分别落在三台机器人上并行执行。 */
    @Test
    void independentTasksRunSimultaneouslyOnDifferentRobots() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("PAR", 0,
                    TaskSpec.builder("A", "MOVE", 10).build(),
                    TaskSpec.builder("B", "MOVE", 10).build(),
                    TaskSpec.builder("C", "MOVE", 10).build());
            LOG.info("INPUT plan with 3 independent tasks, 3 robots available");
            PlanSnapshot snap = orch.submitAndWait(plan);
            LOG.info(orch.explain("PAR"));

            assertEquals(PlanStatus.COMPLETED, snap.status());
            var a = get(snap, "A");
            var b = get(snap, "B");
            var c = get(snap, "C");
            // 三台不同的机器人
            Set<String> robots = new HashSet<>(List.of(
                    a.assignedRobotId(), b.assignedRobotId(), c.assignedRobotId()));
            assertEquals(3, robots.size(),
                    "3 independent tasks must run on 3 different robots");
            // 同一时刻起跑、执行区间两两重叠 => 真的并行而不是串行
            assertEquals(a.startTick(), b.startTick());
            assertEquals(b.startTick(), c.startTick());
            assertTrue(a.endTick() > b.startTick() && b.endTick() > a.startTick(),
                    "A and B intervals must overlap in time");
            LOG.info("PARALLELISM confirmed: A@{} B@{} C@{} all start at tick {}",
                    a.assignedRobotId(), b.assignedRobotId(), c.assignedRobotId(),
                    a.startTick());
        } finally {
            orch.shutdown();
        }
    }

    /** 任务多过机器人时：同一台机器人的执行区间两两不重叠，且整体比纯串行快。 */
    @Test
    void sameRobotNeverDoubleBookedAndParallelismShortensMakespan() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            // 4 个任务 x 10 tick，3 台机器人：串行要 40 tick，并行下 <= 20 tick
            Plan plan = Plan.of("PACK", 0,
                    TaskSpec.builder("T1", "MOVE", 10).build(),
                    TaskSpec.builder("T2", "MOVE", 10).build(),
                    TaskSpec.builder("T3", "MOVE", 10).build(),
                    TaskSpec.builder("T4", "MOVE", 10).build());
            PlanSnapshot snap = orch.submitAndWait(plan);
            LOG.info(orch.explain("PACK"));

            assertEquals(PlanStatus.COMPLETED, snap.status());
            // 按机器人分组，区间两两不重叠
            List<TaskRuntimeView> byRobot = new ArrayList<>(snap.tasks());
            byRobot.sort(Comparator.comparing(TaskRuntimeView::assignedRobotId)
                    .thenComparingLong(TaskRuntimeView::startTick));
            for (int i = 1; i < byRobot.size(); i++) {
                var prev = byRobot.get(i - 1);
                var cur = byRobot.get(i);
                if (prev.assignedRobotId().equals(cur.assignedRobotId())) {
                    assertTrue(cur.startTick() >= prev.endTick(),
                            "robot " + cur.assignedRobotId()
                                    + " double-booked: " + prev.taskId() + " vs " + cur.taskId());
                }
            }
            long makespan = snap.tasks().stream().mapToLong(TaskRuntimeView::endTick)
                    .max().orElseThrow();
            LOG.info("MAKESPAN {} ticks (serial would be 40)", makespan);
            assertTrue(makespan <= 20,
                    "parallel execution must finish 4x10 ticks on 3 robots within 20 ticks, got "
                            + makespan);
        } finally {
            orch.shutdown();
        }
    }

    /** 依赖仍被尊重：并行的两个前置都完成后，后继才开始。 */
    @Test
    void dependenciesStillRespectedUnderParallelism() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("DAG", 0,
                    TaskSpec.builder("A", "MOVE", 6).build(),
                    TaskSpec.builder("B", "MOVE", 10).build(),
                    TaskSpec.builder("C", "MOVE", 2).dependsOn("A", "B").build());
            PlanSnapshot snap = orch.submitAndWait(plan);
            LOG.info(orch.explain("DAG"));

            assertEquals(PlanStatus.COMPLETED, snap.status());
            var a = get(snap, "A");
            var b = get(snap, "B");
            var c = get(snap, "C");
            assertEquals(a.startTick(), b.startTick(), "A and B run in parallel");
            assertTrue(c.startTick() >= Math.max(a.endTick(), b.endTick()),
                    "C must wait for both A and B");
        } finally {
            orch.shutdown();
        }
    }

    /** 相同输入重跑两遍：每步的落机、尝试次数、状态、起止时刻逐字节一致。 */
    @Test
    void parallelOutcomeIsReproducibleForSameInput() {
        String first = runParallelScenario();
        String second = runParallelScenario();
        LOG.info("PARALLEL DETERMINISM identical={}", first.equals(second));
        assertEquals(first, second,
                "same input must reproduce identical assignment/attempts/status/ticks");
    }

    private String runParallelScenario() {
        FleetOrchestrator orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
        try {
            Plan plan = Plan.of("DET", 0,
                    TaskSpec.builder("A", "MOVE", 8).build(),
                    TaskSpec.builder("B", "MOVE", 5).build(),
                    TaskSpec.builder("C", "MOVE", 3).build(),
                    TaskSpec.builder("D", "MOVE", 2).dependsOn("B").build(),
                    TaskSpec.builder("E", "MOVE", 2).dependsOn("A", "C").build());
            PlanSnapshot snap = orch.submitAndWait(plan);
            assertEquals(PlanStatus.COMPLETED, snap.status());
            StringBuilder sb = new StringBuilder();
            for (var t : snap.tasks()) {
                sb.append(t.taskId()).append(':').append(t.status())
                        .append('@').append(t.assignedRobotId())
                        .append('#').append(t.attempts())
                        .append('[').append(t.startTick()).append(',').append(t.endTick())
                        .append(");");
            }
            LOG.info("REPRODUCIBLE final state: {}", sb);
            return sb.toString();
        } finally {
            orch.shutdown();
        }
    }
}
