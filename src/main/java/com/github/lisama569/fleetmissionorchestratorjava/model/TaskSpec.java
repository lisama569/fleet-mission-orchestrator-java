package com.github.lisama569.fleetmissionorchestratorjava.model;

import java.util.List;

/**
 * 任务（步骤）规格。
 * taskId         计划内唯一的任务编号
 * type           任务类型（需与机器人 capability 匹配）
 * durationTicks  执行时长（模拟时钟 tick 数）
 * timeoutTicks   单次尝试超时阈值；非空且小于 durationTicks 时，该尝试在 timeout 时刻被判定 TIMEOUT
 * dependencies   必须先 SUCCEEDED 的前置任务 id
 * priority       数值越大优先级越高（就绪任务排序与高优先级抢占依据）
 * maxAttempts    最大尝试次数（含首次），已完成步骤不会因重试重复生效
 * flakyAttempts  behavior 为 *THEN_SUCCEED 时前多少次尝试不稳定
 * requiredRobotId 非空表示绑定指定机器人；为空表示任意具备能力的机器人
 * deadlineTick   必须在 <= 该 tick 前完成；为空表示无截止
 * behavior       内存模拟执行行为
 */
public record TaskSpec(String taskId,
                       String type,
                       long durationTicks,
                       Long timeoutTicks,
                       List<String> dependencies,
                       int priority,
                       int maxAttempts,
                       int flakyAttempts,
                       String requiredRobotId,
                       Long deadlineTick,
                       TaskBehavior behavior) {

    public TaskSpec {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("type must not be blank");
        }
        if (durationTicks <= 0) {
            throw new IllegalArgumentException("durationTicks must be positive");
        }
        if (timeoutTicks != null && timeoutTicks <= 0) {
            throw new IllegalArgumentException("timeoutTicks must be positive");
        }
        dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (flakyAttempts < 0) {
            throw new IllegalArgumentException("flakyAttempts must be >= 0");
        }
        behavior = behavior == null ? TaskBehavior.SUCCEED : behavior;
    }

    /** 简洁构造器：无绑定机器人、无截止、优先级 0、一次尝试、默认成功。 */
    public static TaskSpec simple(String taskId, String type, long durationTicks,
                                  List<String> dependencies) {
        return new TaskSpec(taskId, type, durationTicks, null, dependencies, 0, 1, 0,
                null, null, TaskBehavior.SUCCEED);
    }

    /** 流畅构造器，便于测试搭建计划。 */
    public static Builder builder(String taskId, String type, long durationTicks) {
        return new Builder(taskId, type, durationTicks);
    }

    public static final class Builder {
        private final String taskId;
        private final String type;
        private final long durationTicks;
        private Long timeoutTicks;
        private List<String> dependencies = List.of();
        private int priority;
        private int maxAttempts = 1;
        private int flakyAttempts;
        private String requiredRobotId;
        private Long deadlineTick;
        private TaskBehavior behavior = TaskBehavior.SUCCEED;

        private Builder(String taskId, String type, long durationTicks) {
            this.taskId = taskId;
            this.type = type;
            this.durationTicks = durationTicks;
        }

        public Builder timeout(long ticks) { this.timeoutTicks = ticks; return this; }
        public Builder dependsOn(String... deps) { this.dependencies = List.of(deps); return this; }
        public Builder priority(int p) { this.priority = p; return this; }
        public Builder maxAttempts(int m) { this.maxAttempts = m; return this; }
        public Builder flakyAttempts(int f) { this.flakyAttempts = f; return this; }
        public Builder requireRobot(String robotId) { this.requiredRobotId = robotId; return this; }
        public Builder deadline(long tick) { this.deadlineTick = tick; return this; }
        public Builder behavior(TaskBehavior b) { this.behavior = b; return this; }

        public TaskSpec build() {
            return new TaskSpec(taskId, type, durationTicks, timeoutTicks, dependencies, priority,
                    maxAttempts, flakyAttempts, requiredRobotId, deadlineTick, behavior);
        }
    }
}
