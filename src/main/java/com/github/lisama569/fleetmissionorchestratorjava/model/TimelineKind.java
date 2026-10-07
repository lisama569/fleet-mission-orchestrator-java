package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 执行时间线上的事件种类。
 * order 用于同一 tick 内多条事件的确定性先后排列（数字小的在前）：
 * 先有计划级触发（追加/重规划），再有抢占/取消/重试，再有完成类结论，最后是新的派发。
 */
public enum TimelineKind {
    APPEND(0),
    REPLAN(0),
    CANCEL(1),
    PREEMPT(1),
    RETRY(1),
    SKIP(2),
    COMPLETE(2),
    EXHAUSTED(2),
    DISPATCH(3);

    private final int order;

    TimelineKind(int order) {
        this.order = order;
    }

    public int order() {
        return order;
    }
}
