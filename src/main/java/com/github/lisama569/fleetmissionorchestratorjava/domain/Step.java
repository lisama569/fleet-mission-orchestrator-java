package com.github.lisama569.fleetmissionorchestratorjava.domain;

import java.util.List;

/**
 * 计划中的单个步骤（运行时实体）。
 *
 * <p>状态流转（全部收敛到确定的终态，不允许从终态回退）：
 * <pre>
 *   PENDING -> ASSIGNED -> RUNNING -> SUCCEEDED
 *                              |
 *                              +-> FAILED     （尝试次数用尽时的终态；否则回到 PENDING 重试）
 *                              +-> TIMED_OUT  （尝试次数用尽时的终态；否则回到 PENDING 重试）
 *   任意非终态 -> CANCELLED
 * </pre>
 *
 * <p>本类的方法不是线程安全的；所有状态变更必须持有 {@code Scheduler} 的全局锁。
 */
public class Step {

    private final String id;
    private final String requiredCapability;
    private final String requiredRobotId;
    private final long durationMillis;
    private final long timeoutMillis;
    private final int maxAttempts;
    private final boolean alwaysFail;
    private final long pinnedFromMillis;
    private final long pinnedToMillis;
    private final List<String> dependencies;

    private StepState state = StepState.PENDING;
    private String assignedRobotId;
    private int attempt;

    public Step(String id, String requiredCapability, String requiredRobotId,
                long durationMillis, long timeoutMillis, int maxAttempts,
                boolean alwaysFail, long pinnedFromMillis, long pinnedToMillis,
                List<String> dependencies) {
        this.id = id;
        this.requiredCapability = requiredCapability;
        this.requiredRobotId = requiredRobotId;
        this.durationMillis = durationMillis;
        this.timeoutMillis = timeoutMillis;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.alwaysFail = alwaysFail;
        this.pinnedFromMillis = pinnedFromMillis;
        this.pinnedToMillis = pinnedToMillis;
        this.dependencies = List.copyOf(dependencies);
    }

    public String getId() { return id; }
    public String getRequiredCapability() { return requiredCapability; }
    public String getRequiredRobotId() { return requiredRobotId; }
    public long getDurationMillis() { return durationMillis; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public int getMaxAttempts() { return maxAttempts; }
    public boolean isAlwaysFail() { return alwaysFail; }
    public long getPinnedFromMillis() { return pinnedFromMillis; }
    public long getPinnedToMillis() { return pinnedToMillis; }
    public List<String> getDependencies() { return dependencies; }

    public StepState getState() { return state; }
    public String getAssignedRobotId() { return assignedRobotId; }
    public int getAttempt() { return attempt; }

    public boolean isPinned() { return pinnedFromMillis < pinnedToMillis; }

    public boolean isTerminal() {
        return state == StepState.SUCCEEDED
                || state == StepState.FAILED
                || state == StepState.TIMED_OUT
                || state == StepState.CANCELLED;
    }

    public boolean isDone() {
        return state == StepState.SUCCEEDED;
    }

    /** 分配给机器人并开始一次新的尝试；只有 PENDING 步骤可被分配。 */
    public void markAssigned(String robotId) {
        if (state != StepState.PENDING) {
            throw new IllegalStateException("step " + id + " not PENDING: " + state);
        }
        this.assignedRobotId = robotId;
        this.attempt++;
        this.state = StepState.ASSIGNED;
    }

    public void markRunning() {
        if (state != StepState.ASSIGNED) {
            throw new IllegalStateException("step " + id + " not ASSIGNED: " + state);
        }
        this.state = StepState.RUNNING;
    }

    /** 完成只允许生效一次：SUCCEEDED 是终态，重复调用直接忽略。 */
    public boolean markSucceeded() {
        if (state == StepState.SUCCEEDED) {
            return false;
        }
        if (state != StepState.RUNNING) {
            throw new IllegalStateException("step " + id + " not RUNNING: " + state);
        }
        this.state = StepState.SUCCEEDED;
        return true;
    }

    /**
     * 执行失败：仍有剩余尝试次数则释放资源、回到 PENDING；否则收敛为 FAILED 终态。
     * @return 是否还会重试
     */
    public boolean handleFailure() {
        if (state != StepState.RUNNING) {
            throw new IllegalStateException("step " + id + " not RUNNING: " + state);
        }
        return retryOrTerminate(StepState.FAILED);
    }

    /**
     * 执行超时：仍有剩余尝试次数则释放资源、回到 PENDING；否则收敛为 TIMED_OUT 终态。
     * @return 是否还会重试
     */
    public boolean handleTimeout() {
        if (state != StepState.RUNNING) {
            throw new IllegalStateException("step " + id + " not RUNNING: " + state);
        }
        return retryOrTerminate(StepState.TIMED_OUT);
    }

    private boolean retryOrTerminate(StepState terminal) {
        if (attempt < maxAttempts) {
            this.state = StepState.PENDING;
            this.assignedRobotId = null;
            return true;
        }
        this.state = terminal;
        this.assignedRobotId = null;
        return false;
    }

    /** 取消：已完成工作保留（SUCCEEDED 不变），其余非终态步骤收敛为 CANCELLED。 */
    public boolean cancel() {
        if (state == StepState.SUCCEEDED || state == StepState.CANCELLED) {
            return false;
        }
        if (isTerminal()) {
            return false;
        }
        this.state = StepState.CANCELLED;
        this.assignedRobotId = null;
        return true;
    }

    /** 机器人在运行中失联时，占用被强制释放：未完成步骤回到 PENDING，尝试次数不增加。 */
    public void preemptToPending() {
        if (isTerminal()) {
            return;
        }
        this.state = StepState.PENDING;
        this.assignedRobotId = null;
    }

    @Override
    public String toString() {
        return "Step{" + id + ", state=" + state + ", attempt=" + attempt
                + (assignedRobotId != null ? ", robot=" + assignedRobotId : "") + '}';
    }
}
