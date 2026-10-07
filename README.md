# 本地车队任务编排与执行流程（内存模拟）

一套**纯内存、无外部服务、无真实凭据**的车队任务编排原型。用内存中的机器人资源模拟任务分配，
覆盖带依赖的计划提交、失败/超时重试、以及运行中的动态重规划，保证并发提交、执行失败与重规划后
的**最终状态始终一致、确定且可解释**。

## 1. 运行与验证

```bash
mvn test           # 运行全部测试（当前 45 个用例，全部通过）
mvn spring-boot:run # 可选：启动本地演示 REST 服务（随机端口 server.port=0）
```

测试日志（SLF4J/Logback 输出到控制台）会打印每个场景的三类信息：

- **INPUT**：提交的计划/任务规格，或注入的运行时事件（机器人下线、取消、高优先级插入）；
- **调度决策**：`DISPATCH`（分配）、`RETRY`（重试）、`INTERRUPT/PREEMPT`（抢占）、
  `REPLAN`（触发重规划）、`SCHEDULE`（可行性排程）；
- **FINAL STATE / EXPLANATION**：每个任务的最终状态、尝试次数、承载机器人、最后结果，
  以及计划级终态。可用 `FleetOrchestrator.explain(planId)` 获取同样的文本。

## 2. 核心抽象

| 类型 | 说明 |
| --- | --- |
| `Robot` | 内存机器人：`id`、能力集合 `capabilities`、是否 `available` |
| `TaskSpec` | 任务规格：类型、时长（tick）、依赖、优先级、最大尝试次数、超时阈值、绑定机器人、截止时刻、模拟行为 |
| `Plan` | 一份带依赖的任务计划（`planId`、到达时刻 `arrivalTick`、任务列表） |
| `PlanValidator` | 静态校验（重复 id / 缺失依赖 / 循环依赖 / 绑定未知机器人） |
| `Scheduler` | 确定性列表调度器（纯函数：相同输入必得相同输出） |
| `TaskExecutionService` | 内存执行器：可中断睡眠模拟时长，按行为产出 SUCCESS/FAILURE/TIMEOUT/PREEMPTED |
| `FleetOrchestrator` | 编排门面：单工作线程事件循环，串行化时钟、调度、状态迁移与重规划 |
| `FleetState` | 全部内存状态（机器人、计划、任务运行时、决策日志、模拟时钟） |

## 3. 调度约束

1. **依赖优先**：任务只有在其所有前置任务都 `SUCCEEDED` 后才可执行；排程保证前置的结束时刻 ≤ 后继的开始时刻。
2. **同一资源同一时刻不重复占用**：每台机器人的分配区间两两不重叠。调度器以
   “固定占用（RUNNING 任务）+ 已排区间”串行化地为任务寻找最早空隙。
3. **能力与绑定**：任务只能由具备对应能力、当前可用的机器人执行；`requiredRobotId`
   非空时只能使用指定机器人（该机器人若下线则任务不可再迁移）。
4. **确定性平局规则**：就绪任务按 `(priority 降序, taskId 升序)`；机器人按
   “能最早开始，其次 robotId 升序”；跨计划按 `(priority 降序, planId 升序, taskId 升序)`。
   不使用系统时间、线程调度顺序或哈希迭代顺序，因此**相同输入必得相同结论**。
5. **截止时刻**：无法在 `deadlineTick` 前完成的计划被拒绝（`RESOURCE_CONFLICT`）。
6. **拒绝原因可区分**（`RejectionReason`）：
   `INVALID_PLAN`、`DUPLICATE_PLAN_ID`、`DUPLICATE_TASK_ID`、`MISSING_DEPENDENCY`、
   `CYCLIC_DEPENDENCY`、`UNKNOWN_REQUIRED_ROBOT`、`NO_CAPABLE_ROBOT`、`RESOURCE_CONFLICT`。

## 4. 任务与计划状态流转

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

- 已完成（`SUCCEEDED`）的步骤**不会因重试或重规划重复生效**：`TaskRuntime`
  禁止从终态迁回非终态。
- 被抢占（机器人下线/取消）**不消耗尝试次数**：运行时立即复位为 `PENDING`、释放机器人，
  由后续调度重新分配。
- 硬超时（`timeoutTicks < durationTicks`）只作用于**首次尝试**；之后的重试不再被硬超时截断，
  从而“超时 → 重试 → 成功”是可收敛的确定路径。行为模型 `TIMEOUT_THEN_SUCCEED`
  用于声明“前 N 次尝试超时，之后成功”。

计划状态（`PlanStatus`）：`ACCEPTED → EXECUTING → COMPLETED / FAILED / CANCELLED`。

- 全部任务 `SUCCEEDED` → `COMPLETED`；
- 全部为 `CANCELLED/SKIPPED` → `CANCELLED`；
- 其余存在非成功终态任务 → `FAILED`（含重规划后无处可去的情形，残余任务收敛为 `SKIPPED`
  并记录 `INFEASIBLE` 原因）。

## 5. 并发模型（结果为何稳定一致）

- 外部线程（提交计划、追加任务、取消、机器人下线、高优先级插入）只把**命令事件**放入一个队列；
  任务执行线程在结束时投递**完成事件**。
- **单个工作线程**串行消费事件：推进模拟时钟、调用纯函数调度器、做状态迁移。
  所有状态读写都在同一把锁内完成。
- 任务的真实执行发生在独立线程池中（仅睡眠，不触碰共享状态），结束后回到事件循环，
  因此并发提交不会产生数据竞争或重复占用。
- **并行派发**：每个 pump 周期循环地把“当前 tick 可启动”的任务逐个下发，已下发的任务
  立即成为其机器人的固定占用，因此同一 tick 可以把多个任务铺到多台机器人上，
  而同一台机器人绝不会被重复占用。
- **命令在飞计数**：工作线程只在“已发起但尚未处理完的外部命令数为 0”时才做派发。
  一波并发提交全部落地后，才基于完整计划集合做一次确定性派发，避免“先到先占”
  让最终结果依赖线程调度。
- **完成事件按模拟时刻成批应用**：完成事件先进入缓冲区；只有当“预期完成时刻不晚于
  缓冲中最早时刻”的在跑任务全部报到后，才把同一模拟时刻的完成事件按
  `(finishTick, launchId)` 排序成批应用、再统一派发。真实世界里谁先醒不影响结论。
- 每次任务启动分配唯一 `launchId`；被抢占后旧尝试迟到的完成事件因 `launchId`
  不匹配而被忽略，绝不污染新一轮执行。

## 6. 动态重规划规则

| 事件 | 规则 |
| --- | --- |
| 机器人下线 `robotUnavailable(id)` | 该机器人移出可用池；其上 RUNNING 任务被中断并立即复位为 PENDING（不计尝试）；未完成部分在下一个 pump 周期重新调度到其它可用机器人；若已无任何可行机器人，计划以可解释原因 `FAILED` 收敛 |
| 任务取消 `cancelTask(plan, task)` | 已 `SUCCEEDED` 的任务取消被忽略（工作不丢失）；未完成任务置 `CANCELLED`，其所有下游级联 `SKIPPED`；其余任务继续执行 |
| 高优先级插入 `insertHighPriority(plan)` | 新计划正常通过校验与可行性检查；**RUNNING 任务不被打断**，高优先级在资源释放后优先获得分配（跨计划按优先级/字典序确定性选择）；已完成工作保留 |

重规划是**增量**的：`SUCCEEDED` 工作视为过去保留，`RUNNING` 工作视为对其机器人的
固定占用，仅对未完成部分重新排程。相同的输入事件序列 ⇒ 相同的最终状态
（见 `DynamicReplanningTest.replanningOutcomeIsReproducibleForSameInputSequence`）。

## 7. 运行中追加任务（appendTasks）

计划接受并跑起来之后，可以继续往里追加新步骤：

```java
orchestrator.appendTasks(planId, List.of(newTask1, newTask2, ...));
```

规则：

- **可以依赖已有步骤**：新步骤的 `dependencies` 可以指向计划里已有的任意步骤
  （包括已完成的）；之后追加的批次也可以依赖上一批追加进来的步骤。
- **已有步骤不可变**：已接受/已完成的步骤不会被修改或删除；追加只往计划末尾加新任务。
- **整体成功或整体不做**：任一新步骤校验失败，整批拒绝，原计划状态不被触碰、
  照常执行，已完成的工作一步不丢。可区分的拒绝原因（`PlanRejectedException.reason()`）：

  | 情形 | RejectionReason |
  | --- | --- |
  | 计划不存在或已到终态 / 批次为空 | `INVALID_PLAN` |
  | 批内重号或与已有步骤重号 | `DUPLICATE_TASK_ID` |
  | 依赖了不存在的步骤 | `MISSING_DEPENDENCY` |
  | 与已有步骤合成环 | `CYCLIC_DEPENDENCY` |
  | 绑定了不存在的机器人 | `UNKNOWN_REQUIRED_ROBOT` |
  | 合并后没有任何机器人能执行 | `NO_CAPABLE_ROBOT` |
  | 合并后怎么排都无法在截止前完成 | `RESOURCE_CONFLICT` |

- **同一套执行规矩**：追加进来的步骤与初始步骤走完全相同的依赖传播、超时与重试、
  被抢占（不计尝试）与重规划逻辑——它们只是计划里“后到的普通步骤”。
- 校验与落库分两段：先对“合并后的候选计划”做静态校验与可行性排程，全部通过才
  修改状态，因此拒绝是原子的，不会留下半截新任务。

## 8. 并行执行

- 同一计划（或不同计划）里彼此没有依赖、且机器人条件满足的步骤，会在**同一个
  模拟 tick 被分别派发到多台机器人上同时执行**，不再是一台干完才轮到下一台。
- 同一台机器人在同一时刻仍然只执行一个任务：已下发任务立即形成对该机器人的
  固定占用，后续派发自动错开。
- 依赖语义不变：后继步骤仍等全部前置 `SUCCEEDED` 后才开始。
- **可复现**：相同输入序列下，每一步落在哪台机器人、执行几次、最终状态、
  起止 tick 完全一致（确定性来自第 5 节的命令在飞计数与完成事件成批机制，
  见 `ParallelExecutionTest.parallelOutcomeIsReproducibleForSameInput`）。

## 9. 执行时间线（timeline）

```java
List<TimelineEntry> timeline = orchestrator.timeline(planId);
```

按模拟时刻排序的结构化事件流，用于复盘与排查：

- 每一步何时被派发（`DISPATCH`，含机器人与第几次尝试）、何时完成（`COMPLETE`）、
  失败重试（`RETRY`）、次数耗尽（`EXHAUSTED`）、被抢占（`INTERRUPT`/`PREEMPT`）、
  依赖失败被跳过（`SKIP`）、取消（`CANCEL`）、重规划（`REPLAN`）、追加（`APPEND`）
  与计划终态（`PLAN-COMPLETED` 等）。
- 事件由单工作线程串行写入、只使用模拟时钟，**相同输入序列重放两遍得到的时间线
  逐条一致**（见 `ExecutionTimelineTest.timelineIsReproducibleAcrossReplays`）。

### 兼容范围

- **对已有计划**：追加是纯增量操作，不追加时行为与之前完全一致；已被接受的计划
  不允许通过追加修改或删除已有步骤，只能新增。已终结（COMPLETED/FAILED/CANCELLED）
  的计划拒绝追加（`INVALID_PLAN`）。
- **对现有用法**：`submit`/`submitAndWait`/`cancelTask`/`robotUnavailable`/
  `insertHighPriority`/`awaitPlan`/`snapshot`/`explain` 的签名与语义不变；
  状态机、拒绝原因枚举、重试与抢占规则不变。并行执行只改变“同一时刻允许跑几个
  任务”，不改变任何单任务的依赖、超时、重试与落机选择规则；相同输入序列下的
  最终状态与旧版一样可复现。
- **验证方式**：`mvn test` 全量回归（45 个用例）；其中 `ConcurrentSubmitTest`、
  `DynamicReplanningTest`、`ExecutionRetryTest` 等既有用例原样通过，
  证明既有行为未被破坏。

## 10. 关键测试

| 测试类 | 覆盖点 |
| --- | --- |
| `PlanValidationTest` | 循环/自环、缺失依赖、重复任务/计划 id、绑定未知机器人、无能力机器人、截止冲突、空计划，逐一断言可区分原因 |
| `SchedulerDeterminismTest` | 相同输入 20 次排程完全一致；机器人区间不重叠；依赖顺序；优先级；并行分配；离线/无能力拒绝 |
| `ExecutionRetryTest` | 先失败后成功（完成步骤只生效一次）、先超时后成功、持续失败耗尽次数且下游 SKIPPED、无重试的硬超时收敛为 TIMED_OUT |
| `ConcurrentSubmitTest` | 6 个计划多线程并发提交全部一致完成，两次运行最终状态逐字节相同；同一 planId 并发提交恰好一个被接受、其余为 DUPLICATE_PLAN_ID |
| `DynamicReplanningTest` | 运行中机器人下线后任务迁移完成且抢占不计次数；取消保留已完成工作并级联 SKIPPED；取消已完成任务无效；高优先级插入不打断 RUNNING 但优先接续；无处可去时以 INFEASIBLE 失败；重规划结论可复现 |
| `PlanAppendTest` | 运行中追加成功并跨批次链式依赖；重号/缺失依赖/成环/未知机器人/无可行方案整批拒绝且原因可区分；拒绝后在跑计划不受影响、已完成工作不丢；追加步骤同样支持重试与机器人下线迁移 |
| `ParallelExecutionTest` | 无依赖步骤真的同时落在多台机器人上（区间重叠、机器人互不相同）；同一机器人不重复占用且并行缩短总时长；依赖语义不变；相同输入重跑结论逐字节一致 |
| `ExecutionTimelineTest` | 时间线按 tick 排序呈现派发/重试/完成/抢占/重规划/追加事件；相同输入序列重放两遍时间线逐条一致 |

## 11. 演示 REST（可选）

- `GET  /api/fleet/plans/{planId}` 查询计划快照
- `POST /api/fleet/plans/{planId}/tasks` 向在跑计划追加一批任务（整体成功或整体拒绝）
- `GET  /api/fleet/plans/{planId}/timeline` 查询执行时间线
- `POST /api/fleet/plans/{planId}/tasks/{taskId}/cancel` 取消任务
- `POST /api/fleet/robots/{robotId}/unavailable` 机器人下线

Bean 装配见 `FleetConfig`（3 台机器人、5ms/tick）。核心编排能力不依赖 Spring，
可直接 `new FleetOrchestrator(robots, millisPerTick)` 在单测中驱动。
