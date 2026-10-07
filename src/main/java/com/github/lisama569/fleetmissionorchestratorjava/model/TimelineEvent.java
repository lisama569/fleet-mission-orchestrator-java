package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 全局执行时间线上的一条事件（按时间先后复盘用）。
 * seq      全局递增序号（同 tick、同种类事件的确定性兜底排序）
 * tick     事件发生的模拟时刻
 * planId   所属计划
 * taskId   关联步骤；计划级事件为空串
 * robotId  关联机器人；不涉及机器人时为空串
 * kind     事件种类
 * attempt  关联的尝试序号（1 起）；不涉及尝试时为 0
 * message  人类可读说明
 */
public record TimelineEvent(long seq,
                            long tick,
                            String planId,
                            String taskId,
                            String robotId,
                            TimelineKind kind,
                            int attempt,
                            String message) {
}
