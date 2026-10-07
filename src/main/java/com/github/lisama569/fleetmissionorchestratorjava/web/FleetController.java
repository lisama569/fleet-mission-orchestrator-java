package com.github.lisama569.fleetmissionorchestratorjava.web;

import com.github.lisama569.fleetmissionorchestratorjava.model.PlanSnapshot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TimelineEvent;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    /**
     * 向运行中的计划整体追加一批步骤（原子：整批成功或整批拒绝）。
     * 请求体为 TaskSpec JSON 数组，例如：
     * [{"taskId":"E","type":"MOVE","durationTicks":2,"dependencies":["D"]}]
     */
    @PostMapping("/plans/{planId}/append")
    public PlanSnapshot append(@PathVariable String planId,
                               @RequestBody List<TaskSpec> tasks) {
        return orchestrator.appendTasks(planId, tasks);
    }

    /** 全局执行时间线（跨计划，按时间先后确定性排序）。 */
    @GetMapping("/timeline")
    public List<TimelineEvent> timeline() {
        return orchestrator.timeline();
    }

    /** 单个计划的执行时间线。 */
    @GetMapping("/plans/{planId}/timeline")
    public List<TimelineEvent> planTimeline(@PathVariable String planId) {
        return orchestrator.timeline(planId);
    }

    @PostMapping("/robots/{robotId}/unavailable")
    public String robotUnavailable(@PathVariable String robotId) {
        orchestrator.robotUnavailable(robotId);
        return "ok";
    }
}
