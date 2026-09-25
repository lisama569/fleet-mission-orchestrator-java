package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 单个任务在执行过程中的可变运行时状态（由编排引擎在锁内维护）。
 */
public final class TaskRuntime {
    private final TaskSpec spec;
    private final String planId;
    private volatile TaskStatus status;
    private volatile int attempts;
    private volatile long startTick;
    private volatile long endTick;
    private volatile String assignedRobotId;
    private volatile AttemptOutcome lastOutcome;

    public TaskRuntime(String planId, TaskSpec spec) {
        this.planId = planId;
        this.spec = spec;
        this.status = TaskStatus.PENDING;
    }

    public TaskSpec spec() { return spec; }
    public String planId() { return planId; }
    public TaskStatus status() { return status; }
    public int attempts() { return attempts; }
    public long startTick() { return startTick; }
    public long endTick() { return endTick; }
    public String assignedRobotId() { return assignedRobotId; }
    public AttemptOutcome lastOutcome() { return lastOutcome; }

    /** 记录一次开始：增加尝试计数并进入 RUNNING。 */
    public void markRunning(String robotId, long tick) {
        this.attempts++;
        this.status = TaskStatus.RUNNING;
        this.assignedRobotId = robotId;
        if (this.startTick == 0) {
            this.startTick = tick;
        }
    }

    /**
     * 状态迁移。已完成（SUCCEEDED/CANCELLED/SKIPPED/FAILED/TIMED_OUT）的步骤
     * 不允许再迁移回非终态，保证“已完成工作不会因重试重复生效”。
     */
    public void transition(TaskStatus next, AttemptOutcome outcome,
                           String robotId, long tick) {
        if (this.status.isTerminal() && next != this.status) {
            throw new IllegalStateException(
                    "terminal task " + spec.taskId() + "(" + this.status
                            + ") cannot transition to " + next);
        }
        this.status = next;
        this.lastOutcome = outcome;
        if (robotId != null) {
            this.assignedRobotId = robotId;
        }
        if (next.isTerminal()) {
            this.endTick = tick;
        }
    }

    public void resetForRetry(String robotId) {
        if (this.status != TaskStatus.RUNNING) {
            throw new IllegalStateException("only RUNNING task can be reset, was " + this.status);
        }
        this.status = TaskStatus.PENDING;
        this.assignedRobotId = robotId;
    }

    /**
     * 被抢占（机器人下线/取消）时立即复位：回到 PENDING、释放机器人，
     * 且这次被打断的尝试不计入尝试次数（抢占不是失败，任务可被重新分配）。
     * 仅当任务仍处于 RUNNING 时生效；若已被标记 CANCELLED 等终态则保持不变。
     */
    public boolean markPreempted() {
        if (this.status != TaskStatus.RUNNING) {
            return false;
        }
        this.status = TaskStatus.PENDING;
        this.assignedRobotId = null;
        this.attempts = Math.max(0, this.attempts - 1);
        this.startTick = 0;
        return true;
    }
}
