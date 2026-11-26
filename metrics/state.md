# 指标：状态存储与对账

覆盖 `states/`：`task_persistents` 的 repo 操作、Redis 账本与 `Reconciler`
周期收敛。指标名与标签常量定义见 [catalog.go](catalog.go)。

## 1. 状态机与指标视角

```
                    Create（落表）
                         │
                         ▼
                      pending ──ClaimPending──▶ dispatching ──MarkDispatched──▶ dispatched
                         ▲                          │                          │
                         │                          │ 发布成功但未确认          │ 执行成功/失败
                         │                          ▼                          ▼
                         └──── Requeue ◀────────────┴────────────────────── terminal
                              （发布失败 / 对账无终态）
```

- **pending**：已落表，等待认领发布。堆积说明发布通道或数据库有问题。
- **dispatching**：已认领。卡住说明发布失败未 Requeue 或进程崩溃。
- **dispatched**：已发布在途。卡住说明执行未回写或 Redis 状态丢失。
- **terminal**：终态。成功由 handler 包装内联回写，失败由对账按 backend 终态收敛。

## 2. 对账

### 2.1 执行流程与采集点

```
Reconciler.Run（ticker）/ entry 首轮 ReconcileOnce
  └─ ① ReconcileRoundsTotal{trigger,result} / ② ReconcileRoundSeconds{trigger}
       ├─ repo.Inflight(now-grace, 100)
       │    ├─ ③ ReconcileInflightRows（上次扫描行数）
       │    └─ 失败 → ④ ReconcileErrorsTotal{stage="inflight"}
       ├─ 逐行 timedOut()
       │    ├─ 未超时 → 跳过
       │    └─ 超时 → ⑤ ReconcileRowsTimedOutTotal{resource}
       │         └─ settle
       │              ├─ backend.GetState → ⑥ ReconcileBackendStateSeconds
       │              │    ├─ 成功态 → MarkTerminal(succeeded) → ⑦ ReconcileSettledTotal{outcome="succeeded"}
       │              │    ├─ 失败态 → MarkTerminal(failed)    → ⑦ {outcome="failed"}
       │              │    └─ 无终态 → Requeue                 → ⑦ {outcome="requeued"}
       │              └─ 各阶段失败 → ④ ReconcileErrorsTotal{stage=...}
       └─ pub.DispatchPending（恢复通道，指标见 scheduler.md）
```

### 2.2 指标定义

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_reconcile_rounds_total` | counter / 1 | `trigger`, `result` | P0 | 对账轮次。`trigger=on_demand`（entry 启动首轮）或 `periodic`（ticker）；`result=ok/error`（本轮任一阶段失败即 error）。 |
| `stateflux_reconcile_round_seconds` | histogram / seconds | `trigger` | P1 | 单轮对账耗时（扫描 + 逐行收敛 + 重派）。P95 接近对账间隔时说明下一轮会延迟。 |
| `stateflux_reconcile_inflight_rows` | gauge / rows | — | P1 | 最近一次 `Inflight` 扫描返回的行数（受 `reconcileBatch=100` 截断）。持续等于 100 说明在途行超过单轮处理能力，需要调大间隔/批大小或排查卡住原因。 |
| `stateflux_reconcile_rows_timed_out_total` | counter / rows | `resource` | P0 | 超过执行预算（`max(timeout_ms, 30s) + grace`）的在途行数。非 0 表示任务卡住、进程崩溃或预算过小。 |
| `stateflux_reconcile_settled_total` | counter / rows | `resource`, `outcome` | P0 | 对账收敛结果。`outcome=succeeded/failed` 表示按 backend 终态落表；`requeued` 表示 backend 无终态（Redis 重启/未执行），行回到 pending 等待重派。 |
| `stateflux_reconcile_errors_total` | counter / 1 | `stage` | P0 | 对账各阶段错误。`stage` 取值：`inflight`（扫描）、`backend_state`（查询执行状态）、`requeue`（回 pending）、`mark_terminal`（写终态）、`ledger`（清理账本）。 |
| `stateflux_reconcile_backend_state_seconds` | histogram / seconds | — | P2 | 单行查询 queue backend 终态的耗时。Redis 变慢时对账轮次会被拉长。 |

### 2.3 查询

```promql
# 对账轮次与耗时
rate(stateflux_reconcile_rounds_total[5m])
histogram_quantile(0.95,
  sum by (le, trigger) (rate(stateflux_reconcile_round_seconds_bucket[5m])))

# 在途扫描量
stateflux_reconcile_inflight_rows

# 超时在途行（按资源）
sum by (resource) (rate(stateflux_reconcile_rows_timed_out_total[5m]))

# 收敛结果分布
sum by (outcome) (rate(stateflux_reconcile_settled_total[5m]))

# 错误阶段
sum by (stage) (rate(stateflux_reconcile_errors_total[5m]))
```

### 2.4 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 对账失败 | `rate(stateflux_reconcile_rounds_total{result="error"}[5m]) > 0` | 扫描或收敛异常 |
| 在途超时 | `rate(stateflux_reconcile_rows_timed_out_total[5m]) > 0` | 任务卡住或执行预算过小 |
| 对账重派激增 | `rate(stateflux_reconcile_settled_total{outcome="requeued"}[5m]) > 1` | Redis 状态丢失或发布未确认，任务将重复执行 |
| 终态写入失败 | `rate(stateflux_reconcile_errors_total{stage="mark_terminal"}[5m]) > 0` | PostgreSQL 异常，在途行无法收敛 |
| 扫描持续饱和 | `stateflux_reconcile_inflight_rows >= 100` | 单轮批大小打满，在途行可能积压 |
| 对账轮次停摆 | `rate(stateflux_reconcile_rounds_total[5m]) == 0` | 对账循环未运行（进程/ctx 异常） |

## 3. 状态存储

### 3.1 指标定义

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_task_rows_created_total` | counter / rows | `resource`, `result` | P1 | `Create` 落表结果。`result=created` 为新建，`duplicate` 为幂等键冲突返回已有行（重复 tick 的正常表现）。落表失败计 `db_operations_total{operation="create",result="error"}`。 |
| `stateflux_task_rows_claimed_total` | counter / rows | `resource` | P1 | `ClaimPending` 认领成功的行数，按行资源计数。与 `publish_rows_claimed_total` 的区别：后者按批次计总量，本指标可按资源拆分。 |
| `stateflux_task_rows_terminal_total` | counter / rows | `resource`, `outcome` | P1 | 写入终态的行数，`outcome=succeeded/failed/dead`。成功主要来自 handler 内联回写，失败/未知来自对账。 |
| `stateflux_task_rows_requeued_total` | counter / rows | `resource`, `reason` | P1 | `Requeue` 回 pending 的行数。`reason=publish_failure`（发布侧组装/投递失败）或 `missing_state`（对账查不到终态）。与 `publish_rows_requeued_total` 互补：前者按行资源，后者按发布批次。 |
| `stateflux_db_operations_total` | counter / 1 | `operation`, `result` | P0 | `task_persistents` 操作次数与结果。`operation=create/get/claim/mark_dispatched/mark_terminal/requeue/inflight`；`result=ok/error`。 |
| `stateflux_db_operation_seconds` | histogram / seconds | `operation` | P0 | `task_persistents` 操作耗时。`claim` 含 `FOR UPDATE SKIP LOCKED` 事务，`inflight` 为扫描查询；P95 升高会直接拖慢发布与对账。 |
| `stateflux_ledger_operations_total` | counter / 1 | `operation`, `result` | P0 | Redis 账本操作。`operation=record`（发布后记账）、`lookup`（按 UUID 查 taskID）、`delete`（终态清理）；`result=ok/error`。账本只是加速索引，失败不阻断主流程，但持续错误会影响排障。 |

### 3.2 查询

```promql
# 落表速率与幂等命中
sum by (resource) (rate(stateflux_task_rows_created_total{result="created"}[5m]))
sum by (resource) (rate(stateflux_task_rows_created_total{result="duplicate"}[5m]))

# 认领与终态速率
sum by (resource) (rate(stateflux_task_rows_claimed_total[5m]))
sum by (outcome) (rate(stateflux_task_rows_terminal_total[5m]))

# 数据库错误率
sum by (operation) (rate(stateflux_db_operations_total{result="error"}[5m]))
  /
sum by (operation) (rate(stateflux_db_operations_total[5m]))

# 数据库操作 P95
histogram_quantile(0.95,
  sum by (le, operation) (rate(stateflux_db_operation_seconds_bucket[5m])))

# 账本错误
sum by (operation) (rate(stateflux_ledger_operations_total{result="error"}[5m]))
```

### 3.3 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 数据库错误 | `rate(stateflux_db_operations_total{result="error"}[5m]) > 0` | 调度事实源异常；发布/对账都会受阻 |
| 认领缓慢 | `histogram_quantile(0.95, sum by (le) (rate(stateflux_db_operation_seconds_bucket{operation="claim"}[5m]))) > 0.5` | 行锁竞争或数据库负载高 |
| 终态写入停滞 | `rate(stateflux_task_rows_terminal_total[5m]) == 0 and rate(stateflux_task_rows_claimed_total[5m]) > 0` | 任务被认领但无一到达终态：检查执行与回写 |
| 重派激增 | `sum(rate(stateflux_task_rows_requeued_total[5m])) > 1` | 发布或 Redis 状态持续失败 |
| 账本错误 | `rate(stateflux_ledger_operations_total{result="error"}[5m]) > 0` | Redis 账本异常（非阻断，但影响追踪） |
