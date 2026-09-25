package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.api.StepSpec;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanState;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Step;
import com.github.lisama569.fleetmissionorchestratorjava.domain.StepState;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.ScheduleDecision;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中动态重规划：
 * 1) 机器人不可用 -> 运行中步骤被抢占并重新分配，已完成工作不丢；
 * 2) 高优先级计划插入 -> 资源一旦释放先服务高优先级；
 * 3) 相同输入序列（含禁用/插入事件的精确时机）重放两次，决策序列必须逐一致。
 */
class ReplanningTest {

    private static final Logger log = LoggerFactory.getLogger(ReplanningTest.class);

    private ValidationRejectionTest.MutableClock clock;
    private Scheduler scheduler;

    private Scheduler initScheduler(ValidationRejectionTest.MutableClock clock) {
        Scheduler s = new Scheduler(clock);
        s.registerRobot(new Robot("R1", Set.of("lift", "move")));
        s.registerRobot(new Robot("R2", Set.of("lift", "move")));
        s.registerRobot(new Robot("R3", Set.of("crane")));
        return s;
    }

    private void init() {
        clock = new ValidationRejectionTest.MutableClock();
        scheduler = initScheduler(clock);
    }

    private StepSpec lift(String id, long duration, List<String> deps) {
        StepSpec s = new StepSpec();
        s.setId(id);
        s.setRequiredCapability("lift");
        s.setDurationMillis(duration);
        s.setMaxAttempts(3);
        s.setDependencies(deps == null ? List.of() : deps);
        return s;
    }

    private PlanSubmission plan(String id, int priority, StepSpec... steps) {
        PlanSubmission p = new PlanSubmission();
        p.setId(id);
        p.setPriority(priority);
        p.setSteps(List.of(steps));
        return p;
    }

    private void tick(long delta) {
        clock.advance(delta);
        scheduler.tick();
    }

    private void runToQuiet() {
        int guard = 0;
        while (!scheduler.isQuiet()) {
            tick(5);
            if (++guard > 200_000) {
                throw new IllegalStateException("调度未收敛");
            }
        }
    }

    private List<String> assignTrace(String stepId) {
        return scheduler.decisions().stream()
                .filter(d -> "ASSIGN_START".equals(d.action()) && stepId.equals(d.stepId()))
                .map(d -> d.planId() + ":" + d.stepId() + "->" + d.robotId())
                .toList();
    }

    @Test
    @DisplayName("机器人失联：S1 从 R1 被抢占，重新分配到 R2 完成，已完成工作不丢")
    void robotDisabledTriggersReassignment() {
        init();
        scheduler.submit(plan("P-A", 0, lift("A1", 100, List.of())));

        log.info("[input] P-A/A1 duration=100ms，两台 lift 机器人 R1/R2");
        tick(5);
        assertEquals(List.of("P-A:A1->R1"), assignTrace("A1"), "确定性分配应选择编号最小的 R1");
                scheduler.disableRobot("R1");
        log.info("[replanning] R1 已禁用，A1 被抢占回 PENDING，尝试次数不计失败");
        Step preempted = scheduler.getPlan("P-A").getStep("A1");
        assertEquals(StepState.PENDING, preempted.getState());
        assertEquals(1, preempted.getAttempt(), "抢占不算失败尝试");

        tick(5); // 重新分配：R1 不可用 -> R2
        log.info("[decision] 抢占后分配轨迹={}", assignTrace("A1"));
        assertEquals(List.of("P-A:A1->R1", "P-A:A1->R2"), assignTrace("A1"));
        assertEquals(StepState.RUNNING, scheduler.getPlan("P-A").getStep("A1").getState());

        runToQuiet();
        Plan plan = scheduler.getPlan("P-A");
        Step a1 = plan.getStep("A1");
        log.info("[final-state] plan={} A1={} finalRobot={} attempts={}",
                plan.getState(), a1.getState(), a1.getAssignedRobotId(), a1.getAttempt());
        assertEquals(PlanState.COMPLETED, plan.getState());
        assertEquals(StepState.SUCCEEDED, a1.getState());
        assertEquals("R2", a1.getAssignedRobotId());
        assertEquals(1, scheduler.decisions().stream()
                .filter(d -> "SUCCEEDED".equals(d.action()) && "A1".equals(d.stepId())).count(),
                "完成效果只能生效一次");
    }

    @Test
    @DisplayName("高优先级插入：R1/R2 忙时插入 P-HIGH，资源释放后 H1 先于低优先级 L3 被分配")
    void highPriorityInsertion() {
        init();
        scheduler.submit(plan("P-LOW", 1,
                lift("L1", 20, List.of()),
                lift("L2", 20, List.of()),
                lift("L3", 20, List.of())));
        log.info("[input] P-LOW 三个并行步骤 L1/L2/L3，只有 R1/R2 两台 lift 机器人");

        tick(5); // L1->R1, L2->R2, L3 等待
        scheduler.submit(plan("P-HIGH", 9, lift("H1", 20, List.of())));
        log.info("[replanning] L1/L2 运行中插入 P-HIGH/H1(priority=9)，运行中任务不被抢占，只影响后续分配");

        runToQuiet();

        List<ScheduleDecision> decisions = scheduler.decisions();
        long h1AssignIndex = indexOfAssign(decisions, "H1");
        long l3AssignIndex = indexOfAssign(decisions, "L3");
        log.info("[decision] H1 分配序号={} L3 分配序号={}", h1AssignIndex, l3AssignIndex);
        assertTrue(h1AssignIndex < l3AssignIndex,
                "资源释放后高优先级 H1 必须先于 L3 被分配");

        for (String pid : List.of("P-LOW", "P-HIGH")) {
            Plan p = scheduler.getPlan(pid);
            log.info("[final-state] {} state={} steps={}", pid, p.getState(),
                    p.getSteps().stream().map(s -> s.getId() + "=" + s.getState()).toList());
            assertEquals(PlanState.COMPLETED, p.getState());
        }
    }

    private long indexOfAssign(List<ScheduleDecision> decisions, String stepId) {
        for (int i = 0; i < decisions.size(); i++) {
            ScheduleDecision d = decisions.get(i);
            if ("ASSIGN_START".equals(d.action()) && stepId.equals(d.stepId())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 相同输入序列可复现：同一段“提交/推进/禁用/插入”脚本在两个全新调度器上各执行一遍，
     * 去掉时间戳后，决策序列必须逐元素一致；最终状态也必须一致。
     */
    @Test
    @DisplayName("确定性重放：相同输入序列两次运行，决策序列逐一致")
    void replanningIsReproducible() {
        List<String> signature1 = runScriptedScenario();
        List<String> signature2 = runScriptedScenario();
        log.info("[decision] 第一次决策签名数={} 第二次决策签名数={}",
                signature1.size(), signature2.size());
        assertEquals(signature1, signature2, "相同输入序列必须产生完全一致的决策序列");
        log.info("[final-state] 两次重放决策序列逐一致，重规划结论可复现");
    }

    private List<String> runScriptedScenario() {
        ValidationRejectionTest.MutableClock c = new ValidationRejectionTest.MutableClock();
        Scheduler s = initScheduler(c);

        Consumer<Long> advance = delta -> {
            c.advance(delta);
            s.tick();
        };

        s.submit(plan("P-A", 0,
                lift("A1", 40, List.of()),
                lift("A2", 30, List.of("A1"))));
        s.submit(plan("P-B", 0, lift("B1", 20, List.of())));
        advance.accept(5L);   // A1->R1
        advance.accept(5L);   // now=10，A1 运行中
        s.disableRobot("R1"); // A1 抢占
        advance.accept(5L);   // now=15，A1->R2（planId 平局 P-A 先于 P-B）
        s.submit(plan("P-D", 9, lift("D1", 20, List.of()))); // 高优先级插入

        int guard = 0;
        while (!s.isQuiet()) {
            advance.accept(5L);
            if (++guard > 200_000) {
                throw new IllegalStateException("调度未收敛");
            }
        }

        Map<String, String> finals = new HashMap<>();
        for (Plan p : s.listPlans()) {
            finals.put(p.getId(), p.getState().name());
        }
        log.info("[final-state] 脚本场景终态={}", finals);
        assertEquals(Set.of("COMPLETED"), Set.copyOf(finals.values()),
                "所有计划最终必须完成");

        // 去掉绝对时间戳，保留动作与实体引用，作为可复现签名
        List<String> signature = new ArrayList<>();
        for (ScheduleDecision d : s.decisions()) {
            signature.add(String.join("|",
                    d.action(),
                    d.planId() == null ? "-" : d.planId(),
                    d.stepId() == null ? "-" : d.stepId(),
                    d.robotId() == null ? "-" : d.robotId()));
        }
        return signature;
    }
}
