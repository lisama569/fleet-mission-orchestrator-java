package com.github.lisama569.fleetmissionorchestratorjava.api;

import java.util.ArrayList;
import java.util.List;

/**
 * 提交计划中单个步骤的请求描述（JSON 绑定用可变 POJO）。
 *
 * @param requiredRobotId    指定机器人 id；与 requiredCapability 二选一或同时给出（取交集）
 * @param requiredCapability 要求机器人具备的能力
 * @param durationMillis     模拟执行时长
 * @param timeoutMillis      超时阈值；<=0 表示不启用超时
 * @param maxAttempts        含首次在内的最大尝试次数（默认 1）
 * @param alwaysFail         true 时该步骤每次执行都失败（用于测试失败/重试）
 * @param pinnedFromMillis   可选：声明的固定占用窗口起点（epoch millis），用于提交期资源冲突检测
 * @param pinnedToMillis     可选：固定占用窗口终点
 * @param dependencies       依赖的前置步骤 id 列表
 */
public class StepSpec {

    private String id;
    private String requiredCapability;
    private String requiredRobotId;
    private long durationMillis;
    private long timeoutMillis;
    private int maxAttempts = 1;
    private boolean alwaysFail;
    private Long pinnedFromMillis;
    private Long pinnedToMillis;
    private List<String> dependencies = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getRequiredCapability() { return requiredCapability; }
    public void setRequiredCapability(String requiredCapability) { this.requiredCapability = requiredCapability; }

    public String getRequiredRobotId() { return requiredRobotId; }
    public void setRequiredRobotId(String requiredRobotId) { this.requiredRobotId = requiredRobotId; }

    public long getDurationMillis() { return durationMillis; }
    public void setDurationMillis(long durationMillis) { this.durationMillis = durationMillis; }

    public long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public boolean isAlwaysFail() { return alwaysFail; }
    public void setAlwaysFail(boolean alwaysFail) { this.alwaysFail = alwaysFail; }

    public Long getPinnedFromMillis() { return pinnedFromMillis; }
    public void setPinnedFromMillis(Long pinnedFromMillis) { this.pinnedFromMillis = pinnedFromMillis; }

    public Long getPinnedToMillis() { return pinnedToMillis; }
    public void setPinnedToMillis(Long pinnedToMillis) { this.pinnedToMillis = pinnedToMillis; }

    public List<String> getDependencies() { return dependencies; }
    public void setDependencies(List<String> dependencies) {
        this.dependencies = dependencies == null ? new ArrayList<>() : dependencies;
    }
}
