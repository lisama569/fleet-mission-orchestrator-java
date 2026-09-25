package com.github.lisama569.fleetmissionorchestratorjava.web;

import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 仅用于本地演示的只读/控制接口。核心能力由 {@link FleetOrchestrator} 直接驱动并由测试覆盖，
 * 不依赖任何外部服务或真实凭据。
 */
@RestController
@RequestMapping("/api/fleet")
public class FleetController {

    private final FleetOrchestrator orchestrator;

    public FleetController(FleetOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @GetMapping("/plans/{planId}")
    public PlanSnapshot get(@PathVariable String planId) {
        return orchestrator.snapshot(planId);
    }

    @PostMapping("/plans/{planId}/tasks/{taskId}/cancel")
    public PlanSnapshot cancel(@PathVariable String planId,
                               @PathVariable String taskId) {
        orchestrator.cancelTask(planId, taskId);
        return orchestrator.snapshot(planId);
    }

    @PostMapping("/robots/{robotId}/unavailable")
    public String robotUnavailable(@PathVariable String robotId) {
        orchestrator.robotUnavailable(robotId);
        return "ok";
    }
}
