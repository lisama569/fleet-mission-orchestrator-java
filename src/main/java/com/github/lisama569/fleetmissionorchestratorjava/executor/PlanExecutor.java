package com.github.lisama569.fleetmissionorchestratorjava.executor;

import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 执行器：以固定的模拟节拍驱动 {@link Scheduler#tick()}。
 *
 * <p>所有状态判断与变更仍然全部在 Scheduler 的全局锁内完成，本组件只是节拍源，
 * 因此墙钟抖动或多节拍重叠（tick 串行化）不会影响最终状态。
 */
@Component
public class PlanExecutor {

    private static final Logger log = LoggerFactory.getLogger(PlanExecutor.class);

    private final Scheduler scheduler;
    private final long tickMillis;
    private final ScheduledExecutorService pump;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public PlanExecutor(Scheduler scheduler,
                        @Value("${fleet.tick-millis:10}") long tickMillis) {
        this.scheduler = scheduler;
        this.tickMillis = tickMillis;
        this.pump = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fleet-executor");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    public void start() {
        if (started.compareAndSet(false, true)) {
            pump.scheduleAtFixedRate(this::safeTick, tickMillis, tickMillis, TimeUnit.MILLISECONDS);
            log.info("[executor] started, tickMillis={}", tickMillis);
        }
    }

    private void safeTick() {
        try {
            scheduler.tick();
        } catch (Throwable t) {
            log.error("[executor] tick error", t);
        }
    }

    /** 立即补一个节拍（测试中可用于精确控制推进时机）。 */
    public void tickOnce() {
        scheduler.tick();
    }

    /** 等待所有计划收敛到终态；超时返回 false，避免测试无限挂起。 */
    public boolean awaitQuiet(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (scheduler.isQuiet()) {
                return true;
            }
            Thread.sleep(Math.max(1, tickMillis / 2));
        }
        return scheduler.isQuiet();
    }

    @PreDestroy
    public void shutdown() {
        pump.shutdownNow();
    }
}
