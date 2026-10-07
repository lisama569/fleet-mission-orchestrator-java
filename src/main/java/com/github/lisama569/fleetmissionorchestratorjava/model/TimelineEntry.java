package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 执行时间线中的一条结构化事件（按模拟时钟 tick 排序后可复盘）。
 * tick       事件发生的模拟时刻
 * eventType   事件类型：DISPATCH / COMPLETE / RETRY / EXHAUSTED / PREEMPT / INTERRUPT /
 *             SKIP / CANCEL / REPLAN / APPEND / INFEASIBLE / PLAN-*
 * planId      所属计划
 * taskId      相关任务（计划级事件为 null）
 * robotId     相关机器人（无则为 null）
 * detail      人类可读的补充说明
 */
public record TimelineEntry(long tick,
                            String eventType,
                            String planId,
                            String taskId,
                            String robotId,
                            String detail) {
}
