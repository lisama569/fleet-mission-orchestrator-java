package com.github.lisama569.fleetmissionorchestratorjava.execution;

import com.github.lisama569.fleetmissionorchestratorjava.model.AttemptOutcome;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内存任务执行器：用可中断的睡眠占用模拟执行时长，按 {@link TaskBehavior}
 * 决定每次尝试结果。不调用任何外部服务或真实凭据。
 *
 * 三种提前结束：
 *  - TIMEOUT：① 硬超时——首次尝试 sleep 超过 timeoutTicks 时被看门狗中断（重试不再受硬超时
 *    截断，以便重试能够恢复）；② 行为模型 TIMEOUT_THEN_SUCCEED 声明前 N 次尝试超时。
 *  - PREEMPTED：编排器在取消/机器人下线/高优先级抢占时中断执行线程。
 *  - 正常完成：按 behavior 得到 SUCCESS / FAILURE 结果。
 */
public final class TaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionService.class);

    private final long millisPerTick;

    public TaskExecutionService(long millisPerTick) {
        if (millisPerTick < 0) {
            throw new IllegalArgumentException("millisPerTick must be >= 0");
        }
        this.millisPerTick = millisPerTick;
    }

    public AttemptOutcome executeAttempt(TaskSpec spec, int attempt, long tick) {
        long sleepMs = millisPerTick * spec.durationTicks();
        long timeoutMs = spec.timeoutTicks() == null ? Long.MAX_VALUE
                : millisPerTick * spec.timeoutTicks();
        // 硬超时只作用于首次尝试：这样“超时 -> 重试 -> 成功”是可以收敛的确定路径。
        boolean enforceTimeout = attempt == 1
                && spec.timeoutTicks() != null
                && spec.timeoutTicks() < spec.durationTicks();

        LOG.info("execute start task={} attempt={} startTick={} durationTicks={} timeoutTicks={} behavior={}",
                spec.taskId(), attempt, tick, spec.durationTicks(), spec.timeoutTicks(), spec.behavior());

        Thread worker = Thread.currentThread();
        AtomicBoolean timedOut = new AtomicBoolean(false);
        Thread watchdog = null;
        if (enforceTimeout && timeoutMs >= 0) {
            watchdog = new Thread(() -> {
                try {
                    Thread.sleep(timeoutMs);
                    timedOut.set(true);
                    worker.interrupt();
                } catch (InterruptedException ignored) {
                    // 正常结束或被抢占，看门狗退出
                }
            }, "exec-watchdog-" + spec.taskId() + "-" + attempt);
            watchdog.setDaemon(true);
            watchdog.start();
        }

        AttemptOutcome outcome;
        try {
            Thread.sleep(sleepMs);
            outcome = spec.behavior().outcomeFor(attempt, spec.flakyAttempts());
        } catch (InterruptedException e) {
            if (timedOut.get()) {
                outcome = AttemptOutcome.TIMEOUT;
            } else {
                Thread.currentThread().interrupt();
                outcome = AttemptOutcome.PREEMPTED;
            }
        } finally {
            if (watchdog != null) {
                watchdog.interrupt();
            }
        }
        LOG.info("execute finish task={} attempt={} outcome={}", spec.taskId(), attempt, outcome);
        return outcome;
    }
}
