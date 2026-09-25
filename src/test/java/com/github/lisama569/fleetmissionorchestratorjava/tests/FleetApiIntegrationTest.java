package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.domain.PlanState;
import com.github.lisama569.fleetmissionorchestratorjava.service.FleetService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：真实 Spring 容器、HTTP 接口与后台执行节拍。
 * 覆盖：提交成功 / 三类拒绝原因可区分 / 后台自动执行收敛 / 机器人禁用重规划 / 取消。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FleetApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(FleetApiIntegrationTest.class);

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private FleetService fleet;

    private Map<String, Object> step(String id, String cap, String robotId, long duration,
                                    int maxAttempts, List<String> deps) {
        return Map.of(
                "id", id,
                "requiredCapability", cap == null ? "" : cap,
                "requiredRobotId", robotId == null ? "" : robotId,
                "durationMillis", duration,
                "timeoutMillis", 0,
                "maxAttempts", maxAttempts,
                "alwaysFail", false,
                "dependencies", deps);
    }

    private Map<String, Object> submission(String id, int priority, List<Map<String, Object>> steps) {
        return Map.of("id", id, "priority", priority, "steps", steps);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> body(ResponseEntity<Map> response) {
        return response.getBody();
    }

    @Test
    @DisplayName("HTTP：合法计划自动执行完成；非法提交返回可区分 reason；禁用/取消接口生效")
    void endToEndOverHttp() throws Exception {
        long ts = System.nanoTime();

        // 1) 合法计划：S1 -> S2，真实节拍驱动下自动完成
        String okId = "IT-OK-" + ts;
        var okSub = submission(okId, 0, List.of(
                step("S1", "lift", null, 30, 1, List.of()),
                step("S2", "crane", null, 30, 1, List.of("S1"))));
        log.info("[input] POST /plans {}", okSub);
        ResponseEntity<Map> resp = rest.postForEntity("/api/fleet/plans", okSub, Map.class);
        log.info("[decision] 提交响应 status={}", resp.getStatusCode());
        assertEquals(HttpStatus.OK, resp.getStatusCode());

        assertTrue(fleet.executor().awaitQuiet(20, TimeUnit.SECONDS), "计划应在超时前收敛");
        PlanState state = fleet.plan(okId).getState();
        log.info("[final-state] {} state={} steps={}", okId, state,
                fleet.plan(okId).getSteps().stream().map(s -> s.getId() + "=" + s.getState()).toList());
        assertEquals(PlanState.COMPLETED, state);

        // 2) 循环依赖 -> CYCLIC_DEPENDENCY
        String cycleId = "IT-CYCLE-" + ts;
        var cycleSub = submission(cycleId, 0, List.of(
                step("S1", "lift", null, 30, 1, List.of("S2")),
                step("S2", "lift", null, 30, 1, List.of("S1"))));
        log.info("[input] POST /plans 循环依赖 id={}", cycleId);
        ResponseEntity<Map> cycleResp = rest.postForEntity("/api/fleet/plans", cycleSub, Map.class);
        log.info("[decision] status={} body={}", cycleResp.getStatusCode(), body(cycleResp));
        assertEquals(HttpStatus.BAD_REQUEST, cycleResp.getStatusCode());
        assertEquals("CYCLIC_DEPENDENCY", body(cycleResp).get("reason"));

        // 3) 无匹配机器人 -> NO_MATCHING_ROBOT
        String robotId = "IT-NOBOT-" + ts;
        var noBotSub = submission(robotId, 0,
                List.of(step("S1", null, "R999", 30, 1, List.of())));
        ResponseEntity<Map> noBotResp = rest.postForEntity("/api/fleet/plans", noBotSub, Map.class);
        log.info("[decision] status={} body={}", noBotResp.getStatusCode(), body(noBotResp));
        assertEquals(HttpStatus.BAD_REQUEST, noBotResp.getStatusCode());
        assertEquals("NO_MATCHING_ROBOT", body(noBotResp).get("reason"));

        // 4) 运行中禁用机器人 -> 抢占；恢复机器人后重新分配并完成
        String disId = "IT-DISABLE-" + ts;
        var disSub = submission(disId, 0,
                List.of(step("S1", "crane", null, 200, 3, List.of())));
        rest.postForEntity("/api/fleet/plans", disSub, Map.class);
        log.info("[input] 已提交长任务 {}，随后禁用 R3", disId);
        Thread.sleep(60); // 让 S1 先在 R3 上运行起来
        try {
            rest.postForEntity("/api/fleet/robots/R3/disable", null, Void.class);
            log.info("[decision] R3 已禁用，S1 被抢占回 PENDING，等待重分配");
            Thread.sleep(120);
            var midPlan = fleet.plan(disId);
            log.info("[replanning] 禁用期间 {} state={} S1={}", disId,
                    midPlan.getState(), midPlan.getStep("S1").getState());
            assertTrue(
                    midPlan.getStep("S1").getState()
                            == com.github.lisama569.fleetmissionorchestratorjava.domain.StepState.PENDING,
                    "R3 是唯一 crane 机器人，禁用后 S1 必须回到 PENDING 等待，且不算失败");

            rest.postForEntity("/api/fleet/robots/R3/enable", null, Void.class);
            log.info("[decision] R3 恢复可用");
            assertTrue(fleet.executor().awaitQuiet(20, TimeUnit.SECONDS), "重分配后应收敛");
            var finalPlan = fleet.plan(disId);
            log.info("[final-state] {} state={} S1={}", disId, finalPlan.getState(),
                    finalPlan.getStep("S1").getState());
            assertEquals(PlanState.COMPLETED, finalPlan.getState(), "抢占后重分配仍应完成");
        } finally {
            fleet.enableRobot("R3");
            fleet.cancelPlan(disId);
        }
    }
}
