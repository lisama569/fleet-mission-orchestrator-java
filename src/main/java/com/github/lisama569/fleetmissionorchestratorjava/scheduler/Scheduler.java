package com.github.lisama569.fleetmissionorchestratorjava.scheduler;

import com.github.lisama569.fleetmissionorchestratorjava.model.Plan;
import com.github.lisama569.fleetmissionorchestratorjava.model.RejectionReason;
import com.github.lisama569.fleetmissionorchestratorjava.model.Robot;
import com.github.lisama569.fleetmissionorchestratorjava.model.Schedule;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskAssignment;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskRuntime;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskSpec;
import com.github.lisama569.fleetmissionorchestratorjava.model.TaskStatus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 确定性列表调度器。
 *
 * 规则（相同输入必得相同输出，不依赖系统时间/线程调度）：
 * 1. SUCCEEDED 工作视为过去并保留；RUNNING 工作视为对其机器人的固定占用（不被抢占）。
 * 2. 反复挑选“已就绪”的未完成任务：前置均已（或将）先完成，且不早于计划到达时刻；
 *    就绪队列按 (priority 降序, taskId 升序) 排序。
 * 3. 为任务选机器人：满足能力与绑定约束的可用机器人中能最早开始者，开始时刻相同按 robotId 升序。
 * 4. 机器人在同一时刻绝不会被重复占用（固定区间 + 已排区间串行化）。
 * 5. 找不到胜任机器人 -> NO_CAPABLE_ROBOT；有胜任机器人但无法在截止前完成 -> RESOURCE_CONFLICT。
 */
public final class Scheduler {

    public static String key(String planId, String taskId) {
        return planId + "::" + taskId;
    }

    /** 一段固定或已排的机器人占用。 */
    private record Reservation(String robotId, long start, long end,
                               String planId, String taskId, int attempt) {}

    /** 提交时的全新可行性调度（尚不存在任何运行时状态）。 */
    public Schedule schedule(Plan plan, Map<String, Robot> robots, long baseTick) {
        return compute(plan, robots, baseTick, List.of(), Map.of());
    }

    /**
     * 重规划。
     * @param runtimes 全部计划的任务运行时，key = planId::taskId；本计划的已完成工作被保留，
     *                 其它计划的 RUNNING 工作形成固定占用。
     */
    public Schedule reschedule(Plan plan,
                               Map<String, Robot> robots,
                               List<Plan> acceptedPlans,
                               Map<String, TaskRuntime> runtimes,
                               long baseTick) {
        List<Reservation> fixed = new ArrayList<>();
        for (Plan p : acceptedPlans) {
            for (TaskSpec t : p.tasks()) {
                TaskRuntime rt = runtimes.get(key(p.planId(), t.taskId()));
                if (rt != null && rt.status() == TaskStatus.RUNNING
                        && rt.assignedRobotId() != null
                        && rt.startTick() + t.durationTicks() > baseTick) {
                    fixed.add(new Reservation(rt.assignedRobotId(), rt.startTick(),
                            rt.startTick() + t.durationTicks(),
                            p.planId(), t.taskId(), rt.attempts()));
                }
            }
        }
        return compute(plan, robots, baseTick, fixed, runtimes);
    }

    private Schedule compute(Plan plan,
                             Map<String, Robot> robots,
                             long baseTick,
                             List<Reservation> initialFixed,
                             Map<String, TaskRuntime> runtimes) {
        List<Reservation> reservations = new ArrayList<>(initialFixed);
        Map<String, Long> finish = new HashMap<>();
        List<TaskAssignment> assignments = new ArrayList<>();

        for (Reservation r : initialFixed) {
            if (r.planId().equals(plan.planId())) {
                finish.put(r.taskId(), r.end());
                assignments.add(new TaskAssignment(r.planId(), r.taskId(), r.robotId(),
                        r.start(), r.end(), r.attempt()));
            }
        }

        Set<String> remaining = new HashSet<>();
        for (TaskSpec t : plan.tasks()) {
            TaskRuntime rt = runtimes.get(key(plan.planId(), t.taskId()));
            if (rt == null) {
                remaining.add(t.taskId());
            } else if (rt.status() == TaskStatus.SUCCEEDED) {
                finish.put(t.taskId(), rt.endTick() > 0 ? rt.endTick() : baseTick);
            } else if (rt.status() == TaskStatus.RUNNING) {
                finish.put(t.taskId(), rt.startTick() + t.durationTicks());
            } else if (rt.status().isTerminal()) {
                // FAILED/TIMED_OUT/CANCELLED/SKIPPED 不参与分配
            } else {
                remaining.add(t.taskId());
            }
        }

        Map<String, TaskSpec> byId = new HashMap<>();
        plan.tasks().forEach(t -> byId.put(t.taskId(), t));

        List<TaskSpec> ready = new ArrayList<>();
        while (!remaining.isEmpty()) {
            ready.clear();
            for (String id : remaining) {
                TaskSpec t = byId.get(id);
                boolean ok = true;
                for (String dep : t.dependencies()) {
                    if (!finish.containsKey(dep)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    ready.add(t);
                }
            }
            if (ready.isEmpty()) {
                return Schedule.rejected(plan.planId(), RejectionReason.RESOURCE_CONFLICT);
            }
            ready.sort(Comparator.comparingInt(TaskSpec::priority).reversed()
                    .thenComparing(TaskSpec::taskId));

            TaskSpec chosen = ready.get(0);
            long depsFinish = chosen.dependencies().stream()
                    .mapToLong(d -> finish.getOrDefault(d, baseTick))
                    .max().orElse(baseTick);
            long earliest = Math.max(baseTick, Math.max(plan.arrivalTick(), depsFinish));

            Reservation best = pickRobot(chosen, robots, reservations, earliest);
            if (best == null) {
                boolean noCapable = capableRobots(chosen, robots).isEmpty();
                return Schedule.rejected(plan.planId(),
                        noCapable ? RejectionReason.NO_CAPABLE_ROBOT : RejectionReason.RESOURCE_CONFLICT);
            }
            if (chosen.deadlineTick() != null && best.end() > chosen.deadlineTick()) {
                return Schedule.rejected(plan.planId(), RejectionReason.RESOURCE_CONFLICT);
            }
            reservations.add(best);
            remaining.remove(chosen.taskId());
            finish.put(chosen.taskId(), best.end());
            assignments.add(new TaskAssignment(plan.planId(), chosen.taskId(), best.robotId(),
                    best.start(), best.end(), best.attempt()));
        }

        assignments.sort(Comparator.comparingLong(TaskAssignment::startTick)
                .thenComparing(TaskAssignment::taskId));
        return Schedule.accepted(plan.planId(), assignments, finish);
    }

    private List<Robot> capableRobots(TaskSpec t, Map<String, Robot> robots) {
        return robots.values().stream()
                .filter(r -> r.available() && r.capabilities().contains(t.type())
                        && (t.requiredRobotId() == null
                        || t.requiredRobotId().equals(r.id())))
                .sorted(Comparator.comparing(Robot::id))
                .toList();
    }

    /**
     * 在 earliest 之后为任务选择能最早执行的机器人区间；通过串行化该机器人已有占用寻找空隙，
     * 保证同一机器人同一时刻不重叠。
     */
    private Reservation pickRobot(TaskSpec t, Map<String, Robot> robots,
                                  List<Reservation> reservations, long earliest) {
        Reservation best = null;
        for (Robot r : capableRobots(t, robots)) {
            List<Reservation> mine = reservations.stream()
                    .filter(x -> x.robotId().equals(r.id()))
                    .sorted(Comparator.comparingLong(Reservation::start))
                    .toList();
            long cursor = earliest;
            for (Reservation x : mine) {
                if (x.end() <= cursor) {
                    continue;
                }
                if (cursor + t.durationTicks() <= x.start()) {
                    break;
                }
                cursor = Math.max(cursor, x.end());
            }
            long start = cursor;
            long end = start + t.durationTicks();
            if (best == null || start < best.start()
                    || (start == best.start() && r.id().compareTo(best.robotId()) < 0)) {
                best = new Reservation(r.id(), start, end, "", t.taskId(), 1);
            }
        }
        return best;
    }
}
