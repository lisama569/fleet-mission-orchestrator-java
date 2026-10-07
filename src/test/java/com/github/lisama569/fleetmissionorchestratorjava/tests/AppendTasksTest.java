package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException;
import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntimeView;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中追加步骤：
 *  - 追加成功：新步骤可依赖已有步骤，再追加的步骤可依赖之前追加的步骤，最终全部完成
 *  - 整批原子拒绝：批次内重复编号 / 与已接受冲突 / 缺依赖 / 成环 / 未知机器人 /
 *    无可行排程 / 计划已终态，原因各自可区分
 *  - 被拒绝时已完成工作不丢、原计划照常收敛
 *  - 追加步骤与初始步骤遵守同样的失败重试规则
 */
class AppendTasksTest {

    private static final Logger LOG = LoggerFactory.getLogger(AppendTasksTest.class);
    private static final long MS_PER_TICK = 2;

    private FleetOrchestrator orch;

    private List<Robot> fleet() {
        return List.of(
                Robot.online("R1", "MOVE"),
                Robot.online("R2", "MOVE"),
                Robot.online("R3", "MOVE"));
    }

    @BeforeEach
    void setUp() {
        orch = new FleetOrchestrator(fleet(), MS_PER_TICK);
        orch.start();
    }

    @AfterEach
    void tearDown() {
        orch.shutdown();
    }

    private TaskRuntimeView get(PlanSnapshot snap, String taskId) {
        return snap.tasks().stream().filter(t -> t.taskId().equals(taskId)).findFirst().orElseThrow();
    }

    private void waitForStatus(String planId, String taskId, TaskStatus expected) {
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            var t = orch.snapshot(planId).tasks().stream()
                    .filter(x -> x.taskId().equals(taskId)).findFirst();
            if (t.isPresent() && t.get().status() == expected) {
                return;
            }
            try { Thread.sleep(3); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new IllegalStateException("task " + taskId + " never reached " + expected);
    }

    /**
     * 追加成功：A 正在跑时追加 B（依赖 A）；A 完成后再追加 C（依赖之前追加的 B），
     * 最终全部 SUCCEEDED，且依赖顺序与并行调度结论确定。
     */
    @Test
    void appendsNewTasksWhilePlanRunsAndChainsAcrossBatches() {
        Plan plan = Plan.of("APPEND-OK", 0,
                TaskSpec.builder("A", "MOVE", 60).build());
        orch.submit(plan);
        waitForStatus("APPEND-OK", "A", TaskStatus.RUNNING);
        LOG.info("INPUT append batch#1 while A running: B depends on A");

        PlanSnapshot after1 = orch.appendTasks("APPEND-OK",
                List.of(TaskSpec.builder("B", "MOVE", 20).dependsOn("A").build()));
        assertEquals(2, after1.tasks().size());
        waitForStatus("APPEND-OK", "A", TaskStatus.SUCCEEDED);
        // B 时长 40ms，趁它仍在运行时再追加 C（依赖 B）
        waitForStatus("APPEND-OK", "B", TaskStatus.RUNNING);

        LOG.info("INPUT append batch#2 while B running: C depends on previously appended B");
        PlanSnapshot after2 = orch.appendTasks("APPEND-OK",
                List.of(TaskSpec.builder("C", "MOVE", 15).dependsOn("B").build()));
        assertEquals(3, after2.tasks().size());

        PlanSnapshot fin = orch.awaitPlan("APPEND-OK");
        LOG.info(orch.explain("APPEND-OK"));
        assertEquals(PlanStatus.COMPLETED, fin.status());
        for (String id : List.of("A", "B", "C")) {
            assertEquals(TaskStatus.SUCCEEDED, get(fin, id).status(), id + " must succeed");
        }
        // 依赖顺序必须满足：A 先于 B 先于 C
        assertTrue(get(fin, "A").endTick() <= get(fin, "B").startTick());
        assertTrue(get(fin, "B").endTick() <= get(fin, "C").startTick());
    }

    /** 追加步骤内部编号重复 -> 整批拒绝 DUPLICATE_TASK_ID_IN_APPEND。 */
    @Test
    void rejectsDuplicateIdWithinAppendBatch() {
        orch.submit(Plan.of("DUP-BATCH", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        List<TaskSpec> batch = List.of(
                TaskSpec.builder("X", "MOVE", 2).build(),
                TaskSpec.builder("X", "MOVE", 2).build());
        LOG.info("INPUT append batch with duplicated task id X");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("DUP-BATCH", batch));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.DUPLICATE_TASK_ID_IN_APPEND, ex.reason());
    }

    /** 新步骤编号与已接受步骤冲突 -> TASK_ID_ALREADY_EXISTS（已接受步骤不可顶替）。 */
    @Test
    void rejectsAppendingTaskIdThatAlreadyExists() {
        orch.submit(Plan.of("EXISTING-ID", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        LOG.info("INPUT append task A again (id already accepted)");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("EXISTING-ID",
                        List.of(TaskSpec.builder("A", "MOVE", 2).build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.TASK_ID_ALREADY_EXISTS, ex.reason());
    }

    /** 新步骤依赖不存在的步骤 -> MISSING_DEPENDENCY。 */
    @Test
    void rejectsAppendedTaskDependingOnGhost() {
        orch.submit(Plan.of("APP-GHOST", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        LOG.info("INPUT append task B depending on GHOST");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-GHOST",
                        List.of(TaskSpec.builder("B", "MOVE", 2).dependsOn("GHOST").build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.MISSING_DEPENDENCY, ex.reason());
    }

    /** 追加后合并依赖图成环：同批次两个新步骤 X->Y、Y->X -> CYCLIC_DEPENDENCY。 */
    @Test
    void rejectsAppendThatCreatesCycle() {
        orch.submit(Plan.of("APP-CYCLE", 0,
                TaskSpec.builder("A", "MOVE", 20).build(),
                TaskSpec.builder("B", "MOVE", 15).dependsOn("A").build()));
        waitForStatus("APP-CYCLE", "A", TaskStatus.SUCCEEDED);
        LOG.info("INPUT append batch X->Y, Y->X (cycle within merged graph)");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-CYCLE", List.of(
                        TaskSpec.builder("X", "MOVE", 2).dependsOn("Y").build(),
                        TaskSpec.builder("Y", "MOVE", 2).dependsOn("X").build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.CYCLIC_DEPENDENCY, ex.reason());
    }

    /** 追加步骤绑定未知机器人 -> UNKNOWN_REQUIRED_ROBOT。 */
    @Test
    void rejectsAppendBoundToUnknownRobot() {
        orch.submit(Plan.of("APP-ROBOT", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        LOG.info("INPUT append task bound to unknown robot RX");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-ROBOT",
                        List.of(TaskSpec.builder("E", "MOVE", 2).requireRobot("RX").build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.UNKNOWN_REQUIRED_ROBOT, ex.reason());
    }

    /** 追加后怎么排都不可行：新增步骤无人能执行 -> NO_CAPABLE_ROBOT。 */
    @Test
    void rejectsAppendWithNoCapableRobot() {
        orch.submit(Plan.of("APP-NOCAP", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        LOG.info("INPUT append FLY task with no capable robot");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-NOCAP",
                        List.of(TaskSpec.builder("F", "FLY", 2).build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.NO_CAPABLE_ROBOT, ex.reason());
    }

    /** 追加后无法满足截止时刻 -> RESOURCE_CONFLICT。 */
    @Test
    void rejectsAppendThatCannotMeetDeadline() {
        // 只有一台机器人；A 长时间占用它，追加步骤的截止无法满足。
        FleetOrchestrator solo = new FleetOrchestrator(
                List.of(Robot.online("R1", "MOVE")), MS_PER_TICK);
        solo.start();
        try {
            solo.submit(Plan.of("APP-DEADLINE", 0,
                    TaskSpec.builder("A", "MOVE", 120).requireRobot("R1").build()));
            LOG.info("INPUT append G deadline=10 while R1 busy with A(120)");
            PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                    () -> solo.appendTasks("APP-DEADLINE",
                            List.of(TaskSpec.builder("G", "MOVE", 5)
                                    .requireRobot("R1").deadline(10).build())));
            LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
            assertEquals(RejectionReason.RESOURCE_CONFLICT, ex.reason());
        } finally {
            solo.shutdown();
        }
    }

    /** 计划已收敛为终态后再追加 -> PLAN_NOT_APPENDABLE。 */
    @Test
    void rejectsAppendToCompletedPlan() {
        orch.submitAndWait(Plan.of("APP-DONE", 0, TaskSpec.builder("A", "MOVE", 5).build()));
        LOG.info("INPUT append into already COMPLETED plan");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-DONE",
                        List.of(TaskSpec.builder("H", "MOVE", 2).build())));
        LOG.info("REJECTED reason={} msg={}", ex.reason(), ex.getMessage());
        assertEquals(RejectionReason.PLAN_NOT_APPENDABLE, ex.reason());
    }

    /** 向不存在的计划追加 -> PLAN_NOT_APPENDABLE。 */
    @Test
    void rejectsAppendToUnknownPlan() {
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("NO-SUCH-PLAN",
                        List.of(TaskSpec.builder("H", "MOVE", 2).build())));
        assertEquals(RejectionReason.PLAN_NOT_APPENDABLE, ex.reason());
    }

    /** 空批次追加 -> INVALID_APPEND。 */
    @Test
    void rejectsEmptyAppendBatch() {
        orch.submit(Plan.of("APP-EMPTY", 0, TaskSpec.builder("A", "MOVE", 80).build()));
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-EMPTY", List.of()));
        assertEquals(RejectionReason.INVALID_APPEND, ex.reason());
    }

    /**
     * 追加步骤与初始步骤守同一套失败/重试规则：
     * 追加的 R 声明“先失败 1 次再成功”，maxAttempts=2，最终 SUCCEEDED 且 attempts=2。
     */
    @Test
    void appendedTaskObeysSameRetryRules() {
        orch.submit(Plan.of("APP-RETRY", 0,
                TaskSpec.builder("A", "MOVE", 80).build()));
        waitForStatus("APP-RETRY", "A", TaskStatus.RUNNING);
        orch.appendTasks("APP-RETRY", List.of(
                TaskSpec.builder("R", "MOVE", 2)
                        .dependsOn("A")
                        .behavior(com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior.FAIL_THEN_SUCCEED)
                        .flakyAttempts(1).maxAttempts(2).build()));
        PlanSnapshot fin = orch.awaitPlan("APP-RETRY");
        LOG.info(orch.explain("APP-RETRY"));
        assertEquals(PlanStatus.COMPLETED, fin.status());
        TaskRuntimeView r = get(fin, "R");
        assertEquals(TaskStatus.SUCCEEDED, r.status());
        assertEquals(2, r.attempts(), "appended task must retry exactly like initial tasks");
    }

    /**
     * 被拒绝时原计划照常跑完，已完成的一步都不能丢：
     * A 已完成、B 在跑时提交一个非法批次（依赖幽灵），拒绝后 B 仍然 SUCCEEDED，计划 COMPLETED。
     */
    @Test
    void rejectedAppendLeavesOriginalPlanIntact() {
        orch.submit(Plan.of("APP-SAFE", 0,
                TaskSpec.builder("A", "MOVE", 10).build(),
                TaskSpec.builder("B", "MOVE", 60).dependsOn("A").build()));
        waitForStatus("APP-SAFE", "A", TaskStatus.SUCCEEDED);
        waitForStatus("APP-SAFE", "B", TaskStatus.RUNNING);
        LOG.info("INPUT invalid append (depends on GHOST) while B is running");
        PlanRejectedException ex = assertThrows(PlanRejectedException.class,
                () -> orch.appendTasks("APP-SAFE",
                        List.of(TaskSpec.builder("Z", "MOVE", 2).dependsOn("GHOST").build())));
        assertEquals(RejectionReason.MISSING_DEPENDENCY, ex.reason());

        PlanSnapshot during = orch.snapshot("APP-SAFE");
        assertEquals(2, during.tasks().size(), "rejected append must not add tasks");
        assertEquals(TaskStatus.SUCCEEDED, get(during, "A").status(),
                "completed work must survive a rejected append");
        assertEquals(TaskStatus.RUNNING, get(during, "B").status(),
                "running task must keep running after a rejected append");

        PlanSnapshot fin = orch.awaitPlan("APP-SAFE");
        LOG.info(orch.explain("APP-SAFE"));
        assertEquals(PlanStatus.COMPLETED, fin.status());
        assertEquals(TaskStatus.SUCCEEDED, get(fin, "A").status());
        assertEquals(TaskStatus.SUCCEEDED, get(fin, "B").status());
    }
}
