package com.github.lisama569.fleetmissionorchestratorjava.web;

import com.github.lisama569.fleetmissionorchestratorjava.api.PlanSubmission;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanValidationException;
import com.github.lisama569.fleetmissionorchestratorjava.domain.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.service.FleetService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 本地车队编排 HTTP 接口（全部操作内存态，无外部依赖）。
 */
@RestController
@RequestMapping("/api/fleet")
public class FleetController {

    private final FleetService fleetService;

    public FleetController(FleetService fleetService) {
        this.fleetService = fleetService;
    }

    /** 提交计划；校验失败返回 400 与可区分的 reason。 */
    @PostMapping("/plans")
    public ResponseEntity<Plan> submit(@RequestBody PlanSubmission submission) {
        return ResponseEntity.ok(fleetService.submit(submission));
    }

    @GetMapping("/plans")
    public List<Plan> plans() {
        return fleetService.plans();
    }

    @GetMapping("/robots")
    public List<Robot> robots() {
        return fleetService.robots();
    }

    /** 机器人不可用：运行中步骤被抢占并重新分配。 */
    @PostMapping("/robots/{robotId}/disable")
    public ResponseEntity<Void> disable(@PathVariable String robotId) {
        fleetService.disableRobot(robotId);
        return ResponseEntity.ok().build();
    }

    /** 恢复机器人可用。 */
    @PostMapping("/robots/{robotId}/enable")
    public ResponseEntity<Void> enable(@PathVariable String robotId) {
        fleetService.enableRobot(robotId);
        return ResponseEntity.ok().build();
    }

    /** 取消计划：已完成步骤保留，未完成步骤收敛为 CANCELLED。 */
    @PostMapping("/plans/{planId}/cancel")
    public ResponseEntity<Void> cancel(@PathVariable String planId) {
        fleetService.cancelPlan(planId);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(PlanValidationException.class)
    public ResponseEntity<Map<String, String>> handle(PlanValidationException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "reason", ex.getReason().name(),
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
