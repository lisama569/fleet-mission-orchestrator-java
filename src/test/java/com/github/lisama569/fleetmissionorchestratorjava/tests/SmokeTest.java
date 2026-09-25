package com.github.lisama569.fleetmissionorchestratorjava.tests;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.PlanStatus;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskBehavior;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.orchestrator.FleetOrchestrator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SmokeTest {

    @Test
    void linearPlanCompletes() {
        FleetOrchestrator orch = new FleetOrchestrator(
                List.of(Robot.online("R1", "MOVE")), 2);
        orch.start();
        try {
            Plan plan = Plan.of("P1", 0,
                    TaskSpec.builder("A", "MOVE", 2).build(),
                    TaskSpec.builder("B", "MOVE", 1).dependsOn("A").build());
            var snap = orch.submitAndWait(plan);
            System.out.println(orch.explain("P1"));
            assertEquals(PlanStatus.COMPLETED, snap.status());
        } finally {
            orch.shutdown();
        }
    }
}
