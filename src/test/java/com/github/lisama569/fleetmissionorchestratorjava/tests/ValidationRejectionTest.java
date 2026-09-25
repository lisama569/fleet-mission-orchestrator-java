package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.api.StepSpec;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanValidationException;
import com.github.lisama569.fleetmissionorchestratorjava.domain.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 输入校验：五类拒绝原因必须可区分。
 * 日志打印输入、拒绝原因；被拒绝的计划不得进入调度器。
 */
class ValidationRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(ValidationRejectionTest.class);

    /** 可手动推进的确定性时钟，避免墙钟导致测试抖动。 */
    static final class MutableClock extends Clock {
        private long millis = 1_000_000L;
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis); }
        @Override public long millis() { return millis; }
        void advance(long delta) { this.millis += delta; }
        void set(long millis) { this.millis = millis; }
    }

    private MutableClock clock;
    private Scheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        scheduler = new Scheduler(clock);
        scheduler.registerRobot(new Robot("R1", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R2", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R3", Set.of("crane")));
    }

    private StepSpec step(String id, String capability, String robotId, long duration,
                          List<String> deps) {
        StepSpec s = new StepSpec();
        s.setId(id);
        s.setRequiredCapability(capability);
        s.setRequiredRobotId(robotId);
        s.setDurationMillis(duration);
        s.setDependencies(new ArrayList<>(deps));
        return s;
    }

    private PlanSubmission plan(String id, int priority, StepSpec... steps) {
        PlanSubmission p = new PlanSubmission();
        p.setId(id);
        p.setPriority(priority);
        p.setSteps(List.of(steps));
        return p;
    }

    private void assertRejected(RejectionReason expected, PlanSubmission submission) {
        log.info("[input] 提交计划 id={} priority={} steps={}",
                submission.getId(), submission.getPriority(),
                submission.getSteps().stream().map(s -> s.getId()
                        + "(deps=" + s.getDependencies()
                        + ", robot=" + s.getRequiredRobotId()
                        + ", cap=" + s.getRequiredCapability() + ")").toList());
        int plansBefore = scheduler.listPlans().size();
        PlanValidationException ex = assertThrows(PlanValidationException.class,
                () -> scheduler.submit(submission));
        log.info("[decision] 计划 {} 被拒绝 reason={} message={}",
                submission.getId(), ex.getReason(), ex.getMessage());
        assertEquals(expected, ex.getReason());
        log.info("[final-state] 调度器中计划数={}（被拒绝计划不得注册，原有计划不受影响）",
                scheduler.listPlans().size());
        assertEquals(plansBefore, scheduler.listPlans().size());
    }

    @Test
    @DisplayName("INVALID_PLAN：空步骤列表 / 重复步骤 id / 非法时长")
    void invalidPlan() {
        assertRejected(RejectionReason.INVALID_PLAN, plan("P-empty", 0));

        PlanSubmission dup = plan("P-dup", 0,
                step("S1", "lift", null, 50, List.of()),
                step("S1", "lift", null, 50, List.of()));
        assertRejected(RejectionReason.INVALID_PLAN, dup);

        StepSpec bad = step("S1", "lift", null, 0, List.of());
        assertRejected(RejectionReason.INVALID_PLAN, plan("P-bad-duration", 0, bad));
    }

    @Test
    @DisplayName("UNKNOWN_DEPENDENCY：依赖引用了计划内不存在的步骤")
    void unknownDependency() {
        PlanSubmission p = plan("P-unknown-dep", 0,
                step("S1", "lift", null, 50, List.of("GHOST")));
        assertRejected(RejectionReason.UNKNOWN_DEPENDENCY, p);
    }

    @Test
    @DisplayName("CYCLIC_DEPENDENCY：S1->S2->S3->S1 循环依赖")
    void cyclicDependency() {
        PlanSubmission p = plan("P-cycle", 0,
                step("S1", "lift", null, 50, List.of("S3")),
                step("S2", "lift", null, 50, List.of("S1")),
                step("S3", "lift", null, 50, List.of("S2")));
        assertRejected(RejectionReason.CYCLIC_DEPENDENCY, p);
    }

    @Test
    @DisplayName("NO_MATCHING_ROBOT：要求不存在的机器人 / 车队不具备的能力")
    void noMatchingRobot() {
        assertRejected(RejectionReason.NO_MATCHING_ROBOT,
                plan("P-no-robot", 0, step("S1", null, "R9", 50, List.of())));

        assertRejected(RejectionReason.NO_MATCHING_ROBOT,
                plan("P-no-cap", 0, step("S1", "fly", null, 50, List.of())));

        // 同时指定：R1 不具备 crane 能力，交集为空
        assertRejected(RejectionReason.NO_MATCHING_ROBOT,
                plan("P-intersection-empty", 0, step("S1", "crane", "R1", 50, List.of())));
    }

    @Test
    @DisplayName("RESOURCE_CONFLICT：固定时间窗在同一机器人上重叠")
    void resourceConflict() {
        StepSpec a = step("S1", null, "R1", 50, List.of());
        a.setPinnedFromMillis(1000L);
        a.setPinnedToMillis(2000L);
        scheduler.submit(plan("P-window-a", 0, a));

        // 与已接受计划的保留窗口重叠
        StepSpec b = step("S1", null, "R1", 50, List.of());
        b.setPinnedFromMillis(1500L);
        b.setPinnedToMillis(1800L);
        assertRejected(RejectionReason.RESOURCE_CONFLICT, plan("P-window-b", 0, b));

        // 同计划内两个固定窗口重叠也拒绝
        StepSpec c1 = step("S1", null, "R1", 50, List.of());
        c1.setPinnedFromMillis(5000L);
        c1.setPinnedToMillis(6000L);
        StepSpec c2 = step("S2", null, "R1", 50, List.of());
        c2.setPinnedFromMillis(5500L);
        c2.setPinnedToMillis(6500L);
        assertRejected(RejectionReason.RESOURCE_CONFLICT, plan("P-window-c", 0, c1, c2));

        // 首尾相接（不重叠）应当被接受
        StepSpec d = step("S1", null, "R1", 50, List.of());
        d.setPinnedFromMillis(2000L);
        d.setPinnedToMillis(3000L);
        scheduler.submit(plan("P-window-d", 0, d));
        log.info("[decision] 首尾相接窗口被接受，当前计划数={}", scheduler.listPlans().size());
        assertEquals(2, scheduler.listPlans().size());
    }
}
