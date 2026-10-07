package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行推进：
 *  - 同一计划内彼此无依赖、机器人条件足够的步骤真的在同一时刻铺到多台机器人上并行执行
 *  - 同一台机器人同一时刻只干一件事
 *  - 相同输入重放两遍：任务->机器人、尝试次数、最终状态与时间线完全一致
 */
class ParallelExecutionTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelExecutionTest.class);
    private static final long MS_PER_TICK = 2;

    private FleetOrchestrator orch;

    @BeforeEach
    void setUp() {
        orch = new FleetOrchestrator(List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE")), MS_PER_TICK);
        orch.start();
    }

    @AfterEach
    void tearDown() {
        orch.shutdown();
    }

    private PlanSnapshot snapshot() {
        return orch.snapshot("PAR");
    }

    private TaskView tv(PlanSnapshot snap, String id) {
        var v = snap.tasks().stream().filter(t -> t.taskId().equals(id)).findFirst().orElseThrow();
        return new TaskView(v.assignedRobotId(), v.status(), v.attempts(),
                v.startTick(), v.endTick());
    }

    private record TaskView(String robot, TaskStatus status, int attempts,
                            long start, long end) {}

    /** 三个互相无依赖的步骤必须在同一 tick 分别落到三台不同机器人上并行执行。 */
    @Test
    void independentTasksRunInParallelOnDifferentRobots() {
        Plan plan = Plan.of("PAR", 0,
                TaskSpec.builder("A", "MOVE", 40).build(),
                TaskSpec.builder("B", "MOVE", 40).build(),
                TaskSpec.builder("C", "MOVE", 40).build());
        LOG.info("INPUT 3 independent 40-tick tasks over 3 robots");
        orch.submit(plan);

        // 轮询直到三个任务同时处于 RUNNING
        long deadline = System.currentTimeMillis() + 8000;
        boolean allParallel = false;
        while (System.currentTimeMillis() < deadline) {
            PlanSnapshot snap = snapshot();
            long running = snap.tasks().stream()
                    .filter(t -> t.status() == TaskStatus.RUNNING).count();
            if (running == 3) {
                Set<String> robots = new HashSet<>();
                snap.tasks().forEach(t -> robots.add(t.assignedRobotId()));
                allParallel = robots.size() == 3;
                if (allParallel) {
                    LOG.info("PARALLEL observed: A,B,C running simultaneously on robots {}",
                            robots);
                    break;
                }
            }
            ThreadSleep();
        }
        assertTrue(allParallel, "A,B,C must run simultaneously on 3 distinct robots");

        PlanSnapshot fin = orch.awaitPlan("PAR");
        LOG.info(orch.explain("PAR"));
        assertEquals(PlanStatus.COMPLETED, fin.status());

        TaskView a = tv(fin, "A");
        TaskView b = tv(fin, "B");
        TaskView c = tv(fin, "C");
        assertEquals(0L, a.start(), "all three start at the same tick");
        assertEquals(0L, b.start());
        assertEquals(0L, c.start());
        Set<String> used = new HashSet<>(List.of(a.robot(), b.robot(), c.robot()));
        assertEquals(3, used.size(), "must occupy 3 different robots");
        // 并行使整体耗时等于单步时长，而不是串行的三倍
        assertEquals(40L, Math.max(a.end(), Math.max(b.end(), c.end())),
                "parallel finish time equals a single task duration");
    }

    /** 只有两台机器人、三个无依赖步骤：同一机器人上的两个区间不得重叠。 */
    @Test
    void sameRobotIsNeverDoubleBooked() {
        FleetOrchestrator two = new FleetOrchestrator(
                List.of(Robot.online("R1", "MOVE"), Robot.online("R2", "MOVE")), MS_PER_TICK);
        two.start();
        try {
            two.submit(Plan.of("PAR", 0,
                    TaskSpec.builder("A", "MOVE", 20).build(),
                    TaskSpec.builder("B", "MOVE", 20).build(),
                    TaskSpec.builder("C", "MOVE", 20).build()));
            PlanSnapshot fin = two.awaitPlan("PAR");
            Map<String, List<TaskView>> byRobot = new HashMap<>();
            for (String id : List.of("A", "B", "C")) {
                var v = fin.tasks().stream().filter(t -> t.taskId().equals(id)).findFirst().orElseThrow();
                byRobot.computeIfAbsent(v.assignedRobotId(), k -> new java.util.ArrayList<>())
                        .add(new TaskView(v.assignedRobotId(), v.status(), v.attempts(),
                                v.startTick(), v.endTick()));
            }
            for (var e : byRobot.entrySet()) {
                List<TaskView> sorted = e.getValue().stream()
                        .sorted((x, y) -> Long.compare(x.start(), y.start())).toList();
                for (int i = 1; i < sorted.size(); i++) {
                    assertTrue(sorted.get(i - 1).end() <= sorted.get(i).start(),
                            "robot " + e.getKey() + " double-booked");
                }
            }
            assertEquals(PlanStatus.COMPLETED, fin.status());
            LOG.info("two-robot schedule respects one-task-at-a-time per robot");
        } finally {
            two.shutdown();
        }
    }

    private String runOnce() {
        FleetOrchestrator o = new FleetOrchestrator(List.of(
                Robot.online("R1", "MOVE"), Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE")), MS_PER_TICK);
        o.start();
        try {
            o.submit(Plan.of("PAR", 0,
                    TaskSpec.builder("A", "MOVE", 12).build(),
                    TaskSpec.builder("B", "MOVE", 12).build(),
                    TaskSpec.builder("C", "MOVE", 12).build(),
                    TaskSpec.builder("D", "MOVE", 8).dependsOn("A").build(),
                    TaskSpec.builder("E", "MOVE", 8).dependsOn("B").build()));
            PlanSnapshot snap = o.awaitPlan("PAR");
            StringBuilder sb = new StringBuilder();
            // TreeMap 保证汇总本身按 taskId 排列，排除遍历顺序干扰
            Map<String, String> rows = new TreeMap<>();
            for (var t : snap.tasks()) {
                rows.put(t.taskId(), t.status() + "@" + t.assignedRobotId()
                        + "#" + t.attempts() + "[" + t.startTick() + "," + t.endTick() + "]");
            }
            rows.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
            for (TimelineEvent e : o.timeline("PAR")) {
                sb.append(e.tick()).append(':').append(e.kind()).append(':')
                        .append(e.taskId()).append('@').append(e.robotId())
                        .append('#').append(e.attempt()).append(';');
            }
            return sb.toString();
        } finally {
            o.shutdown();
        }
    }

    /** 相同输入序列重放两遍：最终分配、尝试次数、时间线必须逐字节一致。 */
    @Test
    void parallelOutcomeAndTimelineAreReproducible() {
        String first = runOnce();
        String second = runOnce();
        LOG.info("PARALLEL DETERMINISM identical={}", first.equals(second));
        assertEquals(first, second, "parallel schedule and timeline must be reproducible");
    }

    private void ThreadSleep() {
        try { Thread.sleep(3); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
