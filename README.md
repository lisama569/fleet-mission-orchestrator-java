# Fleet Mission Orchestrator（本地车队任务编排与执行）

在内存中模拟机器人资源，实现带依赖关系的任务计划提交、确定性调度分配、
执行超时/失败重试与运行中动态重规划。**不依赖任何外部服务或真实凭据**。

## 快速开始

```bash
mvn test          # 运行全部 16 个测试（校验/并发/超时重试/重规划/HTTP 端到端）
mvn spring-boot:run   # 启动服务（随机端口见日志）
```

## 架构

```
web/FleetController      HTTP 接口（提交/查询/禁用/恢复/取消）
service/FleetService     门面：初始化默认车队（R1~R4），转发操作
scheduler/Scheduler      唯一状态变更点：校验、冲突检测、确定性分配、tick 结算
executor/PlanExecutor    节拍源：每 10ms 驱动一次 Scheduler.tick()
domain/                  Plan / Step / Robot / 状态与拒绝原因枚举
```

**并发模型**：所有状态变更都在 `Scheduler` 的同一把全局锁内完成（提交、取消、
禁用机器人、tick 结算）。`PlanExecutor` 只是节拍源，墙钟抖动与节拍重叠不影响
最终状态——相同输入序列必然产生相同的决策序列。

## 调度约束

1. **资源独占**：RUNNING 步骤独占其机器人（`occupiedBy` 表），同一机器人同一时刻
   绝不会被两个步骤占用；测试会回放决策日志逐条验证这一点。
2. **能力/指定机器人匹配**：步骤可声明 `requiredCapability` 和/或 `requiredRobotId`，
   两者都给取交集；都不给则任意可用机器人均可。
3. **确定性分配**：就绪步骤按「优先级降序 → 计划 id 升序 → 步骤 id 升序」排序；
   每个步骤贪心选择「编号最小的空闲且匹配的可用机器人」。分配只依赖状态，
   不依赖线程调度与墙钟。
4. **依赖约束**：步骤仅在其全部依赖 `SUCCEEDED` 后才进入就绪集合。
5. **固定时间窗（可选）**：步骤可声明 `pinnedFromMillis/pinnedToMillis` +
   `requiredRobotId`，提交时与所有已接受计划的保留窗口做区间重叠检测。

## 提交校验与可区分拒绝原因

校验失败抛出 `PlanValidationException`，HTTP 返回 400 与 `reason` 字段：

| reason | 含义 |
|---|---|
| `INVALID_PLAN` | 空计划/空步骤/重复步骤 id/非法时长/非法时间窗/重复计划 id |
| `UNKNOWN_DEPENDENCY` | 依赖引用了计划内不存在的步骤 |
| `CYCLIC_DEPENDENCY` | DFS 三色标记检测到依赖环 |
| `NO_MATCHING_ROBOT` | 指定机器人不存在，或车队不具备要求的能力（含交集为空） |
| `RESOURCE_CONFLICT` | 固定时间窗在同一机器人上与既有保留（或同计划内窗口）重叠 |

被拒绝的计划不会注册，已接受的计划不受影响。

## 状态流转

### 步骤（Step）

```
PENDING --分配--> ASSIGNED --同 tick 启动--> RUNNING --到期--> SUCCEEDED（终态，幂等）
                                              |
                                              +-- 失败/超时且还有剩余尝试 --> PENDING（释放资源，重试）
                                              +-- 失败尝试用尽 --> FAILED（终态）
                                              +-- 超时尝试用尽 --> TIMED_OUT（终态）
任意非终态 --计划取消--> CANCELLED（终态）
RUNNING --机器人失联--> PENDING（抢占，不计失败尝试）
```

- **完成幂等**：`SUCCEEDED` 是终态，重复完成直接忽略，效果只生效一次；
  终态不可回退、不可被取消覆盖。
- **超时判定**：`timeoutMillis > 0` 时，tick 时刻 ≥ 启动时刻+timeout 即判超时，
  且超时优先于成功判定（同一 tick 内先判超时）。

### 计划（Plan，由步骤状态确定性派生）

```
ACCEPTED -> RUNNING -> COMPLETED（全部步骤 SUCCEEDED）
                    -> FAILED_FINAL（任一步骤 FAILED/TIMED_OUT 终态：
                       其余未完成步骤级联取消，已完成步骤保留）
任意未完成阶段 -> CANCELLED（外部取消：已完成步骤保留，其余 CANCELLED）
```

## 重规划规则

| 触发 | 行为 |
|---|---|
| 机器人禁用 `POST /robots/{id}/disable` | 该机器人上 RUNNING 步骤被**抢占**回 PENDING（不计失败尝试、不消耗重试预算），后续 tick 重新分配到其他匹配机器人；已完成步骤不受影响 |
| 机器人恢复 `POST /robots/{id}/enable` | 重新参与分配 |
| 计划取消 `POST /plans/{id}/cancel` | 释放其占用的机器人；已完成步骤保留，其余收敛 CANCELLED；已终态计划忽略 |
| 高优先级计划插入 | **不抢占**运行中的低优先级任务；资源一旦释放，就绪集合按优先级排序，高优先级先被分配 |
| 步骤失败终态 | 同计划其余未完成步骤级联取消（已完成保留），计划 FAILED_FINAL |

**可复现性**：决策只依赖状态与注入的 `Clock`；测试用手动时钟把同一段
「提交/推进/禁用/插入」脚本在两个全新调度器上各执行一遍，断言去掉时间戳后的
决策序列逐元素一致。

## HTTP 接口

```
POST /api/fleet/plans                 提交计划（400 + reason 表示拒绝）
GET  /api/fleet/plans                 查询全部计划
GET  /api/fleet/robots                查询机器人
POST /api/fleet/robots/{id}/disable   机器人失联（触发重规划）
POST /api/fleet/robots/{id}/enable    机器人恢复
POST /api/fleet/plans/{id}/cancel     取消计划
```

提交示例：

```json
{
  "id": "P-1",
  "priority": 5,
  "steps": [
    {"id": "S1", "requiredCapability": "lift", "durationMillis": 200,
     "timeoutMillis": 500, "maxAttempts": 3},
    {"id": "S2", "requiredRobotId": "R3", "durationMillis": 100,
     "dependencies": ["S1"]}
  ]
}
```

## 测试与验证方法

| 测试类 | 覆盖 |
|---|---|
| `ValidationRejectionTest` | 五类拒绝原因可区分；被拒计划不注册、不影响已接受计划 |
| `ConcurrentSubmissionTest` | 20 计划并发提交全部收敛 COMPLETED；同 plan id 并发恰好 1 个成功；决策日志回放验证资源独占；两批运行最终状态一致 |
| `TimeoutRetryTest` | 失败/超时按 maxAttempts 重试后收敛终态；下游级联取消；取消后继续 tick 不复活；Step 级完成幂等 |
| `ReplanningTest` | 机器人失联抢占→重分配→完成；高优先级插入先于低优先级分配；相同输入序列两次重放决策序列逐一致 |
| `FleetApiIntegrationTest` | 真实 Spring 容器 + HTTP：提交/拒绝 reason/后台自动执行/禁用-恢复重分配/取消 |

所有单测日志均打印 `[input]`（输入）、`[decision]`（调度决策）、
`[final-state]`（最终状态）三段，便于人工核对与问题定位。

确定性保障：单元测试使用可手动推进的 `MutableClock` + 单线程 tick，
与墙钟完全解耦；集成测试使用真实节拍但只断言终态。
