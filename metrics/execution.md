# 指标：任务执行、worker 与队列

覆盖 handler 包装（`entry.go` 的 `handlers`/`launchWorkers`）与 `queue/` 的执行
框架（machinery fork）。指标名与标签常量定义见 [catalog.go](catalog.go)。

## 1. 执行流程与采集点

```
entry.launchWorkers
  ├─ ① Workers{resource,node}（每个资源一个 worker）
  ├─ ② WorkerConcurrency{resource}
  └─ worker.LaunchAsync
       └─ broker.StartConsuming
            ├─ ③ QueueMessagesReceivedTotal{queue}（从 Redis 取到消息）
            └─ Process
                 ├─ handler 包装（entry.handlers）
                 │    ├─ ④ TaskExecutionsTotal{resource,result}
                 │    ├─ ⑤ TaskExecutionSeconds{resource,result}
                 │    ├─ 成功 → MarkTerminal
                 │    │    └─ 失败 → ⑥ TaskWritebackErrorsTotal{resource}
                 │    └─ handler 返回 error
                 │         ├─ 还有重试预算 → ⑦ QueueRetriesTotal{queue}
                 │         └─ 重试耗尽     → ⑧ QueueMessagesProcessedTotal{result="failed"}
                 ├─ panic 恢复 → ⑨ QueuePanicsTotal{queue}
                 └─ 终态 → ⑩ QueueMessagesProcessedTotal{queue,result}
       └─ worker 异常退出 → ⑪ WorkerErrorsTotal{resource}
```

队列积压类 Gauge（⑫⑬）不是事件驱动，按抓取或固定周期采样：

```
stateflux_queue_pending_messages{queue}   ← LLEN queue:<resource>/<node>
stateflux_queue_delayed_messages{queue}   ← ZCARD delayed set
```

## 2. 指标定义

### 2.1 handler 执行

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_task_executions_total` | counter / 1 | `resource`, `result` | P0 | handler 执行次数。在 `entry.handlers` 的包装函数里按资源计数；`result=succeeded/failed`。无状态任务（`taskID=0`）也计入。 |
| `stateflux_task_execution_seconds` | histogram / seconds | `resource`, `result` | P0 | handler 执行耗时。不含队列等待、重试退避与回写耗时；用于区分「业务慢」与「队列/发布慢」。 |
| `stateflux_task_writeback_errors_total` | counter / 1 | `resource` | P0 | 执行成功但 `MarkTerminal(succeeded)` 回写失败。回写失败后行仍停留在 dispatched，会被对账重派并重复执行；非 0 必须排查 PostgreSQL。 |
| `stateflux_workers` | gauge / workers | `resource`, `node` | P1 | 本进程已启动的 worker 数。每个资源在本节点一个 worker，正常值为 1；进程停机后归零。 |
| `stateflux_worker_concurrency` | gauge / workers | `resource` | P2 | 每个资源的消费并发（`TaskSpec.Concurrency`，<=0 时为 `DefaultConcurrency`）。用于解释队列积压与执行并行度。 |
| `stateflux_worker_errors_total` | counter / 1 | `resource` | P1 | worker 因 broker 错误退出的次数。非 0 表示该资源的消费已停止，重启或修复 Redis 前任务只会积压。 |

### 2.2 队列

队列名格式为 `queue:<resource>/<node>`，与 `dispatch.QueueName` 一致。

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_queue_messages_received_total` | counter / messages | `queue` | P1 | 从 Redis 取到并交给 worker 的消息数（`BRPOP` 返回）。包含最终失败与重试的消息。 |
| `stateflux_queue_messages_processed_total` | counter / messages | `queue`, `result` | P0 | worker 处理完成的消息数。`result=succeeded/failed/retried`：成功、重试耗尽失败、进入重试。与 `received_total` 的差值反映处理中/阻塞的消息。 |
| `stateflux_queue_retries_total` | counter / 1 | `queue` | P0 | 框架执行内重试次数（`taskRetry`/`retryTaskIn`）。注意这是 queue 层重试，不是对账重派；重派由 `reconcile_settled_total{outcome="requeued"}` 观察。 |
| `stateflux_queue_panics_total` | counter / 1 | `queue` | P0 | handler panic 被框架 recover 的次数。panic 按任务失败处理并走重试；非 0 说明业务代码有 bug。 |
| `stateflux_queue_pending_messages` | gauge / messages | `queue` | P1 | Redis 队列中等待消费的消息数（`LLEN`）。采样获取；持续增长说明消费能力不足或 worker 退出。 |
| `stateflux_queue_delayed_messages` | gauge / messages | `queue` | P1 | 延迟集合中的消息数（重试退避中的任务）。采样获取；持续增长说明失败率高于重试消化能力。 |
| `stateflux_queue_broker_errors_total` | counter / 1 | `operation` | P2 | Redis broker 操作失败次数。`operation` 建议取值：`publish`、`consume`、`delayed_requeue`、`unmarshal`。用于区分「发布失败」与「消费失败」。 |

## 3. 查询

```promql
# 执行吞吐与失败率
sum by (resource) (rate(stateflux_task_executions_total{result="succeeded"}[5m]))
sum by (resource) (rate(stateflux_task_executions_total{result="failed"}[5m]))
  /
sum by (resource) (rate(stateflux_task_executions_total[5m]))

# 执行耗时 P95 / P99
histogram_quantile(0.95,
  sum by (le, resource) (rate(stateflux_task_execution_seconds_bucket[5m])))

# 队列积压（按队列）
sum by (queue) (stateflux_queue_pending_messages)

# 延迟重试积压
sum by (queue) (stateflux_queue_delayed_messages)

# 重试速率
sum by (queue) (rate(stateflux_queue_retries_total[5m]))

# worker 是否全部在线
sum by (resource) (stateflux_workers)
```

## 4. 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 执行失败率过高 | `sum(rate(stateflux_task_executions_total{result="failed"}[5m])) / sum(rate(stateflux_task_executions_total[5m])) > 0.05` | 业务 handler 或下游异常 |
| 回写失败 | `rate(stateflux_task_writeback_errors_total[5m]) > 0` | 成功任务无法落终态，会被重复执行 |
| worker 退出 | `increase(stateflux_worker_errors_total[5m]) > 0` | 消费停止，队列将积压 |
| worker 缺失 | `sum by (resource) (stateflux_workers) < 1` | 资源未启动 worker（配置/注册问题） |
| 队列积压 | `sum by (queue) (stateflux_queue_pending_messages) > 1000` | 消费能力不足或 worker 停止；阈值按业务调整 |
| 延迟集合增长 | `sum by (queue) (stateflux_queue_delayed_messages) > 100` | 失败率过高，重试退避积压 |
| 持续 panic | `rate(stateflux_queue_panics_total[5m]) > 0` | 业务 handler panic，需修复代码 |
| 消费停滞 | `sum(rate(stateflux_queue_messages_received_total[5m])) == 0 and sum(stateflux_queue_pending_messages) > 0` | 有积压但取不到消息：Redis/worker 异常 |

## 5. 与对账的边界

- **执行内重试**（`queue_retries_total`）：queue 框架在同一 Signature 上重发，
  有次数上限（`TaskSpec.Retries`），退避为斐波那契秒级。
- **对账重派**（`reconcile_settled_total{outcome="requeued"}`）：表行回到
  pending 后重新认领发布，无次数上限；仅在 Redis 状态丢失或发布未确认时发生。
- 两者都会导致重复执行，消费端必须按 `IdempotencyKey` 幂等。指标上不要把
  两者相加当作「重试次数」，它们的语义与告警阈值不同。
