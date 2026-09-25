package com.github.lisama569.fleetmissionorchestratorjava.model;

import java.util.Set;

/**
 * 内存中的机器人资源。
 * id          机器人编号
 * capabilities 可执行的任务类型集合
 * available   是否可用（动态重规划中可将其置为不可用）
 */
public record Robot(String id, Set<String> capabilities, boolean available) {

    public Robot(String id, Set<String> capabilities, boolean available) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("robot id must not be blank");
        }
        this.id = id;
        this.capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
        this.available = available;
    }

    /** 可用机器人构造器。 */
    public static Robot online(String id, String... capabilities) {
        return new Robot(id, Set.of(capabilities), true);
    }

    /** 下线（不可用）的同名机器人。 */
    public Robot offline() {
        return new Robot(id, capabilities, false);
    }

    public boolean canRun(String taskType) {
        return available && capabilities.contains(taskType);
    }
}
