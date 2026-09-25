package com.github.lisama569.fleetmissionorchestratorjava;

import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 本地演示用的 Bean 装配：一个三机器人车队 + 每个模拟 tick 对应 5ms 真实时间。
 * 纯内存，无外部依赖。
 */
@Configuration
public class FleetConfig {

    private FleetOrchestrator orchestrator;

    @Bean
    public FleetOrchestrator fleetOrchestrator() {
        List<Robot> robots = List.of(
                Robot.online("R1", "MOVE", "SCAN"),
                Robot.online("R2", "MOVE", "LIFT"),
                Robot.online("R3", "SCAN", "LIFT", "CHARGE"));
        orchestrator = new FleetOrchestrator(robots, 5);
        orchestrator.start();
        return orchestrator;
    }

    @PreDestroy
    public void stop() {
        if (orchestrator != null) {
            orchestrator.shutdown();
        }
    }
}
