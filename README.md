# 本地车队任务编排与执行流程（内存模拟）

一套**纯内存、无外部服务、无真实凭据**的车队任务编排原型。用内存中的机器人资源模拟任务分配，
覆盖带依赖的计划提交、失败/超时重试、运行中的动态重规划、**运行中追加步骤**、
**多机器人真并行推进**与**可复盘的执行时间线**，保证并发提交、执行失败、追加与重规划后
的**最终状态始终一致、确定且可解释**。

## 1. 运行与验证

```bash
mvn test            # 运行全部测试（50 个用例，全部通过）
mvn spring-boot:run # 可选：启动本地演示 REST 服务（随机端口 server.port=0）
```

测试日志（SLF4J/Logback 输出到控制台）会打印每个场景的三类信息：

- **INPUT**：提交的计划/任务规格、追加的步骤批次，或注入的运行时事件（机器人下线、取消、高优先级插入）；
- **调度决策**：`DISPATCH`（分配）、`RETRY`（重试）、`INTERRUPT/PREEMPT`（抢占）、
  `REPLAN`（触发重规划）、`SCHEDULE/SCHEDULE-APPEND`（可行性排程）；
- **FINAL STATE / EXPLANATION**：每个任务的最终状态、尝试次数、承载机器人、最后结果，
  以及计划级终态。可用 `FleetOrchestrator.explain(planId)` 获取同样的文本，
  用 `FleetOrchestrator.timeline()` / `timeline(planId)` 获取执行时间线。

## 2. 核心抽象

| 类型 | 说明 |
| --- | --- |
| `Robot` | 内存机器人：`id`、能力集合 `capabilities`、是否 `available` |
| `TaskSpec` | 任务规格：类型、时长（tick）、依赖、优先级、最大尝试次数、超时阈值、绑定机器人、截止时刻、模拟行为 |
| `Plan` | 一份带依赖的任务计划（`planId`、到达时刻 `arrivalTick`、任务列表）；`withAppendedTasks` 产生追加后的整体计划 |
| `PlanValidator` | 静态校验（重复 id / 缺失依赖 / 循环依赖 / 绑定未知机器人）；`validateAppend` 专管追加批次 |
| `Scheduler` | 确定性列表调度器（纯函数：相同输入必得相同输出） |
| `TaskExecutionService` | 内存执行器：可中断睡眠模拟时长，按行为产出 SUCCESS/FAILURE/TIMEOUT/PREEMPTED |
| `FleetOrchestrator` | 编排门面：单工作线程事件循环 + tick 屏障，串行化时钟、调度、状态迁移与重规划，多机器人真并行 |
| `FleetState` | 全部内存状态（机器人、计划、任务运行时、决策日志、全局时间线、模拟时钟） |
| `TimelineEvent` / `TimelineKind` | 执行时间线事件及其种类（DISPATCH/COMPLETE/RETRY/PREEMPT/CANCEL/SKIP/REPLAN/APPEND/EXHAUSTED） |

## 3. 调度约束

1. **依赖优先**：任务只有在其所有前置任务都 `SUCCEEDED` 后才可执行；排程保证前置的结束时刻 ≤ 后继的开始时刻。
2. **同一资源同一时刻不重复占用**：每台机器人同一时刻只执行一个步骤。调度器以
   “固定占用（RUNNING 任务）+ 已排区间”串行化地为任务寻找最早空隙；运行时由
   “忙机器人集合”保证同一 tick 不会把两个步骤派到同一台机器人。
3. **能力与绑定**：任务只能由具备对应能力、当前可用的机器人执行；`requiredRobotId`
   非空时只能使用指定机器人（该机器人若下线则任务不可再迁移）。
4. **确定性平局规则**：就绪任务按 `(priority 降序, taskId 升序)`；机器人按
   “能最早开始，其次 robotId 升序”；跨计划按 `(priority 降序, planId 升序, taskId 升序)`。
   不使用系统时间、线程调度顺序或哈希迭代顺序，因此**相同输入必得相同结论**。
5. **截止时刻**：无法在 `deadlineTick` 前完成的计划（或追加批次）被拒绝（`RESOURCE_CONFLICT`）。
6. **拒绝原因可区分**（`RejectionReason`）：
   `INVALID_PLAN`、`DUPLICATE_PLAN_ID`、`DUPLICATE_TASK_ID`、`MISSING_DEPENDENCY`、
   `CYCLIC_DEPENDENCY`、`UNKNOWN_REQUIRED_ROBOT`、`NO_CAPABLE_ROBOT`、`RESOURCE_CONFLICT`，
   以及追加场景专用的 `INVALID_APPEND`、`DUPLICATE_TASK_ID_IN_APPEND`、
   `TASK_ID_ALREADY_EXISTS`、`PLAN_NOT_APPENDABLE`。

## 4. 多机器人真并行推进

- 计划接受后，彼此没有依赖关系、且机器人能力/数量条件足够的步骤会在**同一模拟时刻**
  分别铺到多台机器人上真正同时执行（各自独立执行线程，仅睡眠不触碰共享状态）。
  整体耗时因此接近“关键路径”，而不是所有步骤时长之和。
- 同一台机器人在同一时刻仍然只干一件事；后到的步骤在该机器人的最早空隙执行。
- 派发由单工作线程在一个 `pump()` 周期内连续完成：每派出一步就把该机器人标记为忙并重新
  计算确定性调度，直到没有当前 tick 可启动的步骤为止。
- **并行下的确定性由 tick 屏障保证**：在同一模拟时刻结束的多个步骤，其完成事件必须
  全部到齐后，才按 `(planId, taskId, launchId)` 排序一次性落地，再统一派发下一梯队。
  因此真实线程“谁先结束”的时序差异不会影响调度结论——相同输入重放，每一步落在
  哪台机器人、跑了几次、最终状态都一致。

## 5. 运行中追加步骤

`FleetOrchestrator.appendTasks(planId, tasks)`（以及等待收敛的 `appendTasksAndWait`）
允许在计划已经跑起来之后继续往里整体追加一批步骤。

**追加步骤的规则**

- 新步骤可以依赖计划里**已有**（含此前已接受与此前已追加）的步骤；之后再追加的步骤
  也可以反过来依赖本批步骤。
- **不能改动或删除任何已经接受的步骤**：新批次使用了已存在的步骤编号即被拒绝
  （`TASK_ID_ALREADY_EXISTS`），已完成/在跑的步骤永远不会被顶替或回退。
- 追加进来的步骤与初始提交的步骤遵守**完全相同**的一套依赖、超时与重试、被抢占与重规划规则。

**整批原子：要么全部成功，要么整批不做。** 以下情况各自给出可区分的原因，且拒绝时
不写入任何状态，原计划照常接着跑完、已完成的部分一步都不丢：

| 情况 | 原因 |
| --- | --- |
| 新批次为空 | `INVALID_APPEND` |
| 新批次内部出现重复步骤编号 | `DUPLICATE_TASK_ID_IN_APPEND` |
| 新编号与已接受步骤冲突 | `TASK_ID_ALREADY_EXISTS` |
| 依赖了合并后计划中不存在的步骤 | `MISSING_DEPENDENCY` |
| 合并后的依赖图成环 | `CYCLIC_DEPENDENCY` |
| 绑定了不存在的机器人 | `UNKNOWN_REQUIRED_ROBOT` |
| 新增步骤无人能执行 | `NO_CAPABLE_ROBOT` |
| 加进去后怎么排都排不出可行方案（含截止冲突） | `RESOURCE_CONFLICT` |
| 目标计划不存在或已收敛为终态 | `PLAN_NOT_APPENDABLE` |

被接受后：已完成工作视为过去保留、在跑工作视为对其机器人的固定占用，仅对“未完成部分
+ 新步骤”重新做整体确定性排程。

## 6. 任务与计划状态流转

任务状态（`TaskStatus`）：

```
                成功
 PENDING ──► RUNNING ───────► SUCCEEDED          （终态，永不回退/不重复生效）
    ▲           │ 失败/超时（还有剩余次数）
    │           ▼
    └──── 重试（回到 PENDING）
    │           │ 失败/超时（次数耗尽）
    │           ▼
    │        FAILED / TIMED_OUT                  （终态）
    │
    ├─ 取消 ─────────► CANCELLED                  （终态）
    └─ 前置失败/取消/超时 ─► SKIPPED              （终态，下游级联）
```

- 已完成（`SUCCEEDED`）的步骤**不会因重试、重规划或追加重复生效**：`TaskRuntime`
  禁止从终态迁回非终态。
- 被抢占（机器人下线/取消）**不消耗尝试次数**：运行时立即复位为 `PENDING`、释放机器人，
  由后续调度重新分配。
- 硬超时（`timeoutTicks < durationTicks`）只作用于**首次尝试**；之后的重试不再被硬超时截断。
  行为模型 `TIMEOUT_THEN_SUCCEED` 用于声明“前 N 次尝试超时，之后成功”。

计划状态（`PlanStatus`）：`ACCEPTED → EXECUTING → COMPLETED / FAILED / CANCELLED`。
计划进入终态后不可再追加步骤。

## 7. 执行时间线（复盘 / 排查）

`timeline()`（全计划）与 `timeline(planId)`（单计划）返回按时间先后排列的
`TimelineEvent`：`tick`、`planId`、`taskId`、`robotId`、`kind`、`attempt`、`message`，
可看清每一步在什么时刻、由哪台机器人执行、何时结束，以及中途是否发生抢占、重试、
取消、跳过、重规划或追加。

- 排序是确定的：`(tick 升序, 种类顺序, planId, taskId, seq)`。种类顺序把同一 tick 的
  “计划级触发 → 抢占/取消/重试 → 完成结论 → 新派发”排清楚。
- 时间线只记录模拟时钟 tick 与确定性决策，**不含真实墙钟时间**；配合 tick 屏障，
  同一个输入序列重放两遍，时间线逐条对得上。

## 8. 并发模型（结果为何稳定一致）

- 外部线程（提交计划、追加、取消、机器人下线、高优先级插入）只把**命令事件**放入一个队列；
  任务执行线程在结束时投递**完成事件**。
- **单个工作线程**串行消费事件：推进模拟时钟、调用纯函数调度器、做状态迁移。
  所有状态读写都在同一把锁内完成。
- 任务的真实执行发生在独立线程池中（仅睡眠，不触碰共享状态）；多台机器人可同时执行，
  结束后经完成事件与 tick 屏障回到事件循环。
- 每次任务启动分配唯一 `launchId`；被抢占后旧尝试迟到的完成事件因 `launchId`
  不匹配而在屏障入口被直接忽略，绝不污染新一轮执行。

## 9. 动态重规划规则

| 事件 | 规则 |
| --- | --- |
| 机器人下线 `robotUnavailable(id)` | 该机器人移出可用池；其上**所有** RUNNING 任务被中断并立即复位为 PENDING（不计尝试）；未完成部分在下一个 pump 周期重新调度到其它可用机器人；若已无任何可行机器人，计划以可解释原因 `FAILED` 收敛 |
| 任务取消 `cancelTask(plan, task)` | 已 `SUCCEEDED` 的任务取消被忽略（工作不丢失）；未完成任务置 `CANCELLED`，其所有下游级联 `SKIPPED`；其余任务继续执行 |
| 高优先级插入 `insertHighPriority(plan)` | 新计划正常通过校验与可行性检查；**RUNNING 任务不被打断**，高优先级在资源释放后优先获得分配；已完成工作保留 |

重规划是**增量**的：`SUCCEEDED` 工作视为过去保留，`RUNNING` 工作视为对其机器人的
固定占用，仅对未完成部分重新排程。

## 10. 关键测试

| 测试类 | 覆盖点 |
| --- | --- |
| `PlanValidationTest` | 循环/自环、缺失依赖、重复任务/计划 id、绑定未知机器人、无能力机器人、截止冲突、空计划，逐一断言可区分原因 |
| `SchedulerDeterminismTest` | 相同输入 20 次排程完全一致；机器人区间不重叠；依赖顺序；优先级；并行分配；离线/无能力拒绝 |
| `ExecutionRetryTest` | 先失败后成功、先超时后成功、耗尽次数且下游 SKIPPED、无重试硬超时收敛 |
| `ConcurrentSubmitTest` | 6 个计划多线程并发提交全部一致完成，两次运行最终状态逐字节相同；同 id 并发提交恰好一个被接受 |
| `DynamicReplanningTest` | 机器人下线迁移、取消保留已完成工作、取消已完成任务无效、高优先级不打断 RUNNING、无处可去 INFEASIBLE、重规划可复现 |
| `AppendTasksTest` | 运行中追加成功并跨批次链状依赖；批次内重复/与已接受冲突/缺依赖/成环/未知机器人/无能力/截止不可行/计划终态/未知计划/空批次各自可区分拒绝；被拒绝时已完成与在跑工作不受影响；追加步骤同样遵守重试规则 |
| `ParallelExecutionTest` | 三个无依赖步骤同 tick 落在三台不同机器人并行；同一机器人不重叠；相同输入两遍的最终分配与时间线逐字节一致 |
| `TimelineQueryTest` | 时间线含派发/完成/抢占/重规划/重试/追加且按时间有序；相同输入序列重放两遍时间线一致 |

## 11. 演示 REST（可选）

- `GET  /api/fleet/plans/{planId}` 查询计划快照
- `POST /api/fleet/plans/{planId}/append` 运行中整体追加步骤（请求体为 `TaskSpec` JSON 数组，原子成功/拒绝）
- `GET  /api/fleet/timeline` 全局执行时间线
- `GET  /api/fleet/plans/{planId}/timeline` 单计划执行时间线
- `POST /api/fleet/plans/{planId}/tasks/{taskId}/cancel` 取消任务
- `POST /api/fleet/robots/{robotId}/unavailable` 机器人下线

Bean 装配见 `FleetConfig`（3 台机器人、5ms/tick）。核心编排能力不依赖 Spring，
可直接 `new FleetOrchestrator(robots, millisPerTick)` 在单测中驱动。
