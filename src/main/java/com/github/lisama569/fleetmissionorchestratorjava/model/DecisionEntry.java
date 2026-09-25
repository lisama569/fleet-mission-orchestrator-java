package com.github.lisama569.fleetmissionorchestratorjava.model;

/**
 * 编排器记录的一条决策日志（用于“可解释”与测试断言）。
 */
public record DecisionEntry(long tick, String category, String message) {
}
