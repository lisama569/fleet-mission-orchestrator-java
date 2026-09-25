package com.github.lisama569.fleetmissionorchestratorjava.service;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.executor.PlanExecutor;
import com.github.lisama569.fleetmissionorchestratorjava.scheduler.Scheduler;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * 对外门面：初始化内存中的模拟机器人资源、提交计划、触发重规划、查询状态。
 * 不依赖任何外部服务或真实凭据。
 */
@Service
public class FleetService {

    private final Scheduler scheduler;
    private final PlanExecutor executor;

    public FleetService(Scheduler scheduler, PlanExecutor executor) {
        this.scheduler = scheduler;
        this.executor = executor;
    }

    /** 默认车队：R1/R2 通用搬运，R3 起重，R4 巡检。 */
    @PostConstruct
    public void initDefaultFleet() {
        if (!scheduler.listRobots().isEmpty()) {
            return;
        }
        scheduler.registerRobot(new Robot("R1", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R2", Set.of("lift", "move")));
        scheduler.registerRobot(new Robot("R3", Set.of("crane")));
        scheduler.registerRobot(new Robot("R4", Set.of("inspect", "move")));
    }

    public Plan submit(PlanSubmission submission) {
        return scheduler.submit(submission);
    }

    public void disableRobot(String robotId) {
        scheduler.disableRobot(robotId);
    }

    public void enableRobot(String robotId) {
        scheduler.enableRobot(robotId);
    }

    public void cancelPlan(String planId) {
        scheduler.cancelPlan(planId);
    }

    public List<Robot> robots() {
        return scheduler.listRobots();
    }

    public List<Plan> plans() {
        return scheduler.listPlans();
    }

    public Plan plan(String planId) {
        return scheduler.getPlan(planId);
    }

    public PlanExecutor executor() {
        return executor;
    }
}
