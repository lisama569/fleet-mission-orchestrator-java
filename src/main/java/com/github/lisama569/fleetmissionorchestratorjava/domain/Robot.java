package com.github.lisama569.fleetmissionorchestratorjava.domain;

import java.util.Set;

/**
 * 内存模拟的机器人资源。
 */
public class Robot {

    private final String id;
    private final Set<String> capabilities;
    private volatile boolean available;

    public Robot(String id, Set<String> capabilities) {
        this.id = id;
        this.capabilities = Set.copyOf(capabilities);
        this.available = true;
    }

    public String getId() { return id; }
    public Set<String> getCapabilities() { return capabilities; }
    public boolean isAvailable() { return available; }
    public void setAvailable(boolean available) { this.available = available; }
}
