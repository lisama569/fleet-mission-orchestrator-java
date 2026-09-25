package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.Schedule;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskAssignment;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调度器纯函数测试：相同输入必得相同输出；同一机器人任意时刻不会被重复占用；
 * 依赖关系必须满足；高优先级先得资源。
 */
class SchedulerDeterminismTest {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerDeterminismTest.class);

    private Map<String, Robot> robots(Robot... rs) {
        Map<String, Robot> m = new HashMap<>();
        for (Robot r : rs) {
            m.put(r.id(), r);
        }
        return m;
    }

    /** 检查任意同一机器人的分配区间互不重叠。 */
    private void assertNoOverlap(Schedule s) {
        Map<String, List<TaskAssignment>> byRobot = new HashMap<>();
        for (TaskAssignment a : s.assignments()) {
            if (a.robotId() != null) {
                byRobot.computeIfAbsent(a.robotId(), k -> new ArrayList<>()).add(a);
            }
        }
        for (var e : byRobot.entrySet()) {
            List<TaskAssignment> sorted = e.getValue().stream()
                    .sorted(java.util.Comparator.comparingLong(TaskAssignment::startTick))
                    .toList();
            for (int i = 1; i < sorted.size(); i++) {
                TaskAssignment prev = sorted.get(i - 1);
                TaskAssignment cur = sorted.get(i);
                assertTrue(prev.endTick() <= cur.startTick(),
                        "robot " + e.getKey() + " double-booked: " + prev + " vs " + cur);
            }
        }
    }

    private void assertDepsRespected(Plan p, Schedule s) {
        Map<String, TaskAssignment> byTask = new HashMap<>();
        for (TaskAssignment a : s.assignments()) {
            byTask.put(a.taskId(), a);
        }
        for (TaskSpec t : p.tasks()) {
            TaskAssignment cur = byTask.get(t.taskId());
            for (String dep : t.dependencies()) {
                TaskAssignment d = byTask.get(dep);
                assertTrue(d.endTick() <= cur.startTick(),
                        "dep " + dep + " must finish before " + t.taskId());
            }
        }
    }

    @Test
    void sameInputProducesSameSchedule() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> pool = robots(
                Robot.online("R1", "MOVE"), Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
        Plan plan = Plan.of("P", 0,
                TaskSpec.builder("A", "MOVE", 3).priority(1).build(),
                TaskSpec.builder("B", "MOVE", 2).priority(5).build(),
                TaskSpec.builder("C", "MOVE", 1).priority(5).dependsOn("A").build());

        List<String> results = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Schedule s = scheduler.schedule(plan, pool, 0);
            assertTrue(s.feasible());
            results.add(s.assignments().toString());
        }
        long distinct = results.stream().distinct().count();
        LOG.info("INPUT 20x same plan; distinct schedules={}", distinct);
        LOG.info("SCHEDULE {}", results.get(0));
        assertEquals(1, distinct, "identical inputs must yield identical schedules");
    }

    @Test
    void neverDoubleBooksRobot() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> one = robots(Robot.online("SOLO", "MOVE"));
        Plan plan = Plan.of("SERIAL", 0,
                TaskSpec.simple("A", "MOVE", 2, List.of()),
                TaskSpec.simple("B", "MOVE", 2, List.of()),
                TaskSpec.simple("C", "MOVE", 2, List.of()));
        Schedule s = scheduler.schedule(plan, one, 0);
        assertTrue(s.feasible());
        LOG.info("INPUT one robot, 3 independent tasks; SCHEDULE {}", s.assignments());
        assertNoOverlap(s);
        assertEquals(6L, s.finishTickByTask().get("C"));
    }

    @Test
    void respectsDependencies() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> pool = robots(
                Robot.online("R1", "MOVE"), Robot.online("R2", "MOVE"));
        Plan plan = Plan.of("DEPS", 0,
                TaskSpec.simple("A", "MOVE", 2, List.of()),
                TaskSpec.simple("B", "MOVE", 2, List.of("A")),
                TaskSpec.simple("C", "MOVE", 2, List.of("B")));
        Schedule s = scheduler.schedule(plan, pool, 0);
        assertTrue(s.feasible());
        assertDepsRespected(plan, s);
        assertNoOverlap(s);
        LOG.info("INPUT chained deps; SCHEDULE {}", s.assignments());
        assertEquals(6L, s.finishTickByTask().get("C"));
    }

    @Test
    void higherPriorityRunsFirstWhenOnlyOneRobot() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> one = robots(Robot.online("SOLO", "MOVE"));
        Plan plan = Plan.of("PRIO", 0,
                TaskSpec.builder("LOW", "MOVE", 2).priority(1).build(),
                TaskSpec.builder("HIGH", "MOVE", 2).priority(9).build());
        Schedule s = scheduler.schedule(plan, one, 0);
        assertTrue(s.feasible());
        TaskAssignment high = s.assignments().stream()
                .filter(a -> a.taskId().equals("HIGH")).findFirst().orElseThrow();
        TaskAssignment low = s.assignments().stream()
                .filter(a -> a.taskId().equals("LOW")).findFirst().orElseThrow();
        LOG.info("INPUT HIGH(p9)/LOW(p1) on one robot; SCHEDULE {}", s.assignments());
        assertEquals(0L, high.startTick());
        assertEquals(2L, low.startTick());
    }

    @Test
    void picksEarliestAvailableAcrossRobots() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> pool = robots(
                Robot.online("R1", "MOVE"), Robot.online("R2", "MOVE"));
        Plan plan = Plan.of("PAR", 0,
                TaskSpec.simple("A", "MOVE", 2, List.of()),
                TaskSpec.simple("B", "MOVE", 2, List.of()));
        Schedule s = scheduler.schedule(plan, pool, 0);
        assertTrue(s.feasible());
        assertNoOverlap(s);
        // 两个任务应并行落在 R1/R2，都从 tick 0 开始
        LOG.info("INPUT 2 tasks / 2 robots; SCHEDULE {}", s.assignments());
        assertEquals(2, s.assignments().size());
        assertTrue(s.assignments().stream().allMatch(a -> a.startTick() == 0));
    }

    @Test
    void infeasibleWhenRobotMissingCapability() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> pool = robots(Robot.online("R1", "MOVE"));
        Plan plan = Plan.of("X", 0, TaskSpec.simple("A", "FLY", 1, List.of()));
        Schedule s = scheduler.schedule(plan, pool, 0);
        assertFalse(s.feasible());
        LOG.info("INPUT task type FLY no capability; REASON {}", s.reason());
        assertEquals(com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason.NO_CAPABLE_ROBOT,
                s.reason());
    }

    @Test
    void offlineRobotIsNotScheduled() {
        Scheduler scheduler = new Scheduler();
        Map<String, Robot> pool = robots(Robot.online("R1", "MOVE").offline());
        Plan plan = Plan.of("OFF", 0, TaskSpec.simple("A", "MOVE", 1, List.of()));
        Schedule s = scheduler.schedule(plan, pool, 0);
        assertFalse(s.feasible());
        LOG.info("INPUT only robot offline; REASON {}", s.reason());
    }
}
