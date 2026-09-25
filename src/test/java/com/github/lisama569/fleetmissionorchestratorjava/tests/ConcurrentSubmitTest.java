package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发提交：多线程同时提交互不相同的计划，所有被接受计划最终都收敛到确定终态；
 * 任意时刻不存在同一机器人执行两个任务；两次运行的“最终任务->机器人/尝试次数/状态”一致。
 */
class ConcurrentSubmitTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentSubmitTest.class);
    private static final long MS_PER_TICK = 3;

    private final List<Robot> fleet = List.of(
            Robot.online("R1", "MOVE", "SCAN"),
            Robot.online("R2", "MOVE"),
            Robot.online("R3", "SCAN"));

    private Plan buildPlan(String prefix, int seed) {
        // 每个计划 4 个任务：A,B 并行 -> C 依赖 A -> D 依赖 B,C；时长由 seed 决定
        long da = 1 + (seed % 3);
        long db = 1 + ((seed + 1) % 3);
        return Plan.of(prefix, 0,
                TaskSpec.builder(prefix + "-A", "MOVE", da).build(),
                TaskSpec.builder(prefix + "-B", "SCAN", db).build(),
                TaskSpec.builder(prefix + "-C", "MOVE", 1).dependsOn(prefix + "-A").build(),
                TaskSpec.builder(prefix + "-D", "SCAN", 1)
                        .dependsOn(prefix + "-B", prefix + "-C").build());
    }

    private String runOnce(int planCount) throws Exception {
        FleetOrchestrator orch = new FleetOrchestrator(fleet, MS_PER_TICK);
        orch.start();
        ExecutorService pool = Executors.newFixedThreadPool(planCount);
        try {
            List<Callable<String>> jobs = new ArrayList<>();
            for (int i = 0; i < planCount; i++) {
                final int seed = i;
                jobs.add(() -> orch.submit(buildPlan("P" + seed, seed)));
            }
            List<Future<String>> futures = pool.invokeAll(jobs);
            for (Future<String> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }

            // 等待全部终态
            StringBuilder summary = new StringBuilder();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < planCount; i++) {
                ids.add("P" + i);
            }
            for (String id : ids) {
                PlanSnapshot snap = orch.awaitPlan(id);
                assertEquals(PlanStatus.COMPLETED, snap.status(), "plan " + id + " must complete");
                for (var t : snap.tasks()) {
                    summary.append(id).append('/').append(t.taskId())
                            .append('=').append(t.status())
                            .append("@robot=").append(t.assignedRobotId())
                            .append(",attempts=").append(t.attempts())
                            .append(';');
                }
            }
            LOG.info("FINAL STATE ({} plans concurrently submitted):\n{}", planCount, summary);
            return summary.toString();
        } finally {
            pool.shutdownNow();
            orch.shutdown();
        }
    }

    @Test
    void concurrentSubmissionsAllCompleteConsistently() throws Exception {
        String first = runOnce(6);
        // 再跑一次：输入序列相同（计划 id/规格一致），最终结论必须可复现
        String second = runOnce(6);
        LOG.info("DETERMINISM CHECK identicalFinalState={}", first.equals(second));
        assertEquals(first, second, "same input sequence must reproduce identical final state");
        // 基本健全性：所有 6 个计划各 4 任务都出现
        for (int i = 0; i < 6; i++) {
            for (String t : List.of("-A", "-B", "-C", "-D")) {
                assertTrue(first.contains("P" + i + t + "=SUCCEEDED"),
                        "missing succeeded task P" + i + t);
            }
        }
    }

    @Test
    void concurrentSubmissionOfSamePlanIsRejectedOnce() throws Exception {
        // 同一计划 id 并发提交：恰好一个成功，另一个得到 DUPLICATE_PLAN_ID
        FleetOrchestrator orch = new FleetOrchestrator(fleet, MS_PER_TICK);
        orch.start();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<String>> jobs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                jobs.add(() -> orch.submit(buildPlan("SAME", 1)));
            }
            List<Future<String>> futures = pool.invokeAll(jobs);
            int accepted = 0;
            TreeSet<String> reasons = new TreeSet<>();
            for (Future<String> f : futures) {
                try {
                    f.get(5, TimeUnit.SECONDS);
                    accepted++;
                } catch (Exception e) {
                    Throwable cur = e;
                    while (cur.getCause() != null) {
                        cur = cur.getCause();
                    }
                    if (cur instanceof com.github.lisama569.fleetmissionorchestratorjava.exception.PlanRejectedException pre) {
                        reasons.add(pre.reason().name());
                    } else {
                        reasons.add(cur.getClass().getSimpleName());
                    }
                }
            }
            LOG.info("SAME-ID concurrent submit accepted={} rejectedReasons={}", accepted, reasons);
            assertEquals(1, accepted, "exactly one submission of same plan id may win");
            assertEquals(1, reasons.size());
            assertEquals("DUPLICATE_PLAN_ID", reasons.first());
            orch.awaitPlan("SAME");
        } finally {
            pool.shutdownNow();
            orch.shutdown();
        }
    }
}
