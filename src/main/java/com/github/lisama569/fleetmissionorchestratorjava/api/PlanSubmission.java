package com.github.lisama569.fleetmissionorchestratorjava.api;

import java.util.ArrayList;
import java.util.List;

/**
 * 计划提交请求。
 *
 * @param id       计划唯一 id
 * @param priority 数值越大优先级越高（用于高优先级插入时的分配排序与抢占）
 * @param steps    步骤列表
 */
public class PlanSubmission {

    private String id;
    private int priority;
    private List<StepSpec> steps = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }

    public List<StepSpec> getSteps() { return steps; }
    public void setSteps(List<StepSpec> steps) {
        this.steps = steps == null ? new ArrayList<>() : steps;
    }
}
