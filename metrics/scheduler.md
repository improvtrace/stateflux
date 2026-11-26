# 指标：定时触发与发布通道

覆盖 `scheduler.go` 的 `Listener`、`CronScheduler` 与 `Publisher`。指标名与
标签常量定义见 [catalog.go](catalog.go)。

## 1. 定时触发

### 1.1 执行流程与采集点

```
cron 到点
  └─ fire()                    ① CronTicksTotal
       ├─ SETNX 失败（Redis）   ② CronLockErrorsTotal
       ├─ 锁已被他节点持有       ③ CronLockSkippedTotal
       └─ 抢锁成功              ④ CronLockAcquiredTotal
            └─ Listener.Emit()  ⑤ CronEventsEmittedTotal / ⑥ CronEventsDroppedTotal
                 └─ Listener.Run()
                      └─ dispatch(handler)
                           ├─ Handler 调用  ⑦ ListenerEventsTotal
                           ├─ panic 恢复    ⑧ ListenerHandlerPanicsTotal
                           └─ HandleEvent   ⑨ CronEventHandleSeconds
                                ├─ 资源未注册        ⑩ CronEventErrorsTotal{stage="lookup"}
                                ├─ Factory 生成失败   ⑪ CronEventErrorsTotal{stage="factory"}
                                └─ 非 Durable 直发失败 ⑫ CronEventErrorsTotal{stage="dispatch"}
```

### 1.2 指标定义

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_cron_registered_tasks` | gauge / tasks | — | P1 | 本进程注册的定时任务数。`NewCronScheduler` 完成 `reg.Timed()` 注册后设置；进程生命周期内不变。若为 0，说明没有任务配置 `Cron`。 |
| `stateflux_cron_ticks_total` | counter / 1 | `resource` | P0 | cron 回调触发次数，**在抢锁之前**计数。它回答「定时器是否在走」，不回答「是否真正触发」。多节点部署时每个节点都会增加。 |
| `stateflux_cron_lock_acquired_total` | counter / 1 | `resource` | P0 | 成功抢到单节点防重锁（Redis `SETNX`）并发出事件的次数。这是「真正触发」的次数。 |
| `stateflux_cron_lock_skipped_total` | counter / 1 | `resource` | P1 | 锁被其它节点持有而跳过的次数。单节点上 `acquired + skipped + errors == ticks`；多节点部署时 skipped 占多数是正常的，持续为 0 反而说明锁未生效。 |
| `stateflux_cron_lock_errors_total` | counter / 1 | `resource` | P0 | 抢锁时 Redis 返回错误。非 0 意味着防重失效，多节点可能重复触发。 |
| `stateflux_cron_events_emitted_total` | counter / 1 | `resource` | P1 | 事件被 listener 缓冲接收的次数（`Emit` 返回 true）。 |
| `stateflux_cron_events_dropped_total` | counter / 1 | `resource` | P0 | listener 缓冲已满、事件被丢弃的次数。丢一次 tick 由下个周期补偿，但持续丢弃说明 handler 太慢或缓冲太小。 |
| `stateflux_cron_event_handle_seconds` | histogram / seconds | `resource`, `result` | P1 | `CronScheduler.HandleEvent` 的总耗时（含 Factory 生成与发布），`result=ok/error`。用于定位「tick 到了但批次迟迟发不出去」。 |
| `stateflux_cron_event_errors_total` | counter / 1 | `resource`, `stage` | P0 | 事件处理失败次数。`stage` 取值：`lookup`（资源未注册）、`factory`（批次生成失败）、`dispatch`（非 Durable 直发失败）。 |
| `stateflux_listener_events_total` | counter / 1 | `event` | P1 | listener 事件循环实际分发的事件数，`event=cron`。与 `cron_events_emitted_total` 的差值反映缓冲中尚未消费的事件。 |
| `stateflux_listener_handler_panics_total` | counter / 1 | `handler` | P1 | 分发事件时 recover 到的 panic 次数。非 0 说明某个 handler 有 bug，但事件循环仍存活。 |

### 1.3 查询

```promql
# 每个资源的触发速率（真正抢到锁）
sum by (resource) (rate(stateflux_cron_lock_acquired_total[5m]))

# 锁竞争/跳过比例：多节点部署时每个 tick 只应有一个节点真正触发
sum by (resource) (rate(stateflux_cron_lock_skipped_total[5m]))
  /
sum by (resource) (rate(stateflux_cron_ticks_total[5m]))

# 事件丢弃速率
sum by (resource) (rate(stateflux_cron_events_dropped_total[5m]))

# 事件处理 P95 耗时
histogram_quantile(0.95,
  sum by (le, resource) (rate(stateflux_cron_event_handle_seconds_bucket[5m])))
```

### 1.4 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 定时器停摆 | `sum(rate(stateflux_cron_ticks_total[5m])) == 0 and sum(stateflux_cron_registered_tasks) > 0` | 注册了定时任务但没有任何 tick：检查 cron 表达式与进程状态 |
| 触发锁错误 | `rate(stateflux_cron_lock_errors_total[5m]) > 0` | Redis 异常，防重失效 |
| 事件持续丢弃 | `rate(stateflux_cron_events_dropped_total[5m]) > 0` | listener 缓冲不足或 handler 阻塞 |
| 批次生成失败 | `rate(stateflux_cron_event_errors_total{stage="factory"}[5m]) > 0` | Factory 数据源或编码异常 |
| 直发失败 | `rate(stateflux_cron_event_errors_total{stage="dispatch"}[5m]) > 0` | 节点路由或队列发布异常 |

## 2. 发布通道

`Publisher` 有两条使用路径，指标按**顶层调用**计数，避免内部调用重复计数：

- `Publisher.Dispatch`（cron 到点）：先逐条落表/直发，再调用内部
  `dispatchPending`。整批计为 `trigger="cron"`。
- `Publisher.DispatchPending`（对账恢复）：由 `Reconciler` 直接调用，整批计为
  `trigger="recovery"`。

### 2.1 两阶段发布与采集点

```
Dispatch / DispatchPending
  └─ ① PublishBatchesTotal{trigger,result}
       ├─ Create 落表         ② PublishTasksTotal{kind="durable"}
       │    └─ 幂等键冲突      ③ TaskRowsCreatedTotal{result="duplicate"}（见 state.md）
       ├─ 直发（无表行）       ④ PublishTasksTotal{kind="stateless"}
       └─ ClaimPending 认领    ⑤ PublishRowsClaimedTotal
            └─ 逐行组装 Signature
                 ├─ 组装失败     ⑥ PublishRowsRequeuedTotal{reason="signature"}
                 └─ Dispatch
                      ├─ 失败    ⑦ PublishRowsRequeuedTotal{reason="dispatch"}
                      └─ 成功    ⑧ PublishRowsDispatchedTotal
       └─ MarkDispatched       ⑨ PublishErrorsTotal{stage="mark_dispatched"}
```

### 2.2 指标定义

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_publish_batches_total` | counter / 1 | `trigger`, `result` | P1 | 顶层发布批次次数。`trigger=cron/recovery`；`result=ok/error`（批次内任一任务失败即 error）。`Publisher.Dispatch` 与 `Reconciler` 调用的 `DispatchPending` 各计一次。 |
| `stateflux_publish_batch_seconds` | histogram / seconds | `trigger` | P1 | 顶层发布批次总耗时，含落表、认领、逐行发布与确认。 |
| `stateflux_publish_batch_rows` | histogram / rows | `trigger` | P2 | 单批处理的任务行数。cron 路径为 Factory 产物数，recovery 路径为认领行数。用于观察批次规模与 `defaultDispatchBatch`（100）的关系。 |
| `stateflux_publish_tasks_total` | counter / tasks | `resource`, `kind`, `result` | P0 | 逐任务发布结果。`kind=durable`（落表）或 `stateless`（直发）；`result=ok/error`。幂等键冲突返回已有行也计 `ok`（重复行由 `task_rows_created_total{result="duplicate"}` 单独观察）。 |
| `stateflux_publish_rows_claimed_total` | counter / rows | — | P0 | `ClaimPending` 返回的行数累计。持续大于 0 且 `publish_rows_dispatched_total` 不增长，说明认领后卡住。 |
| `stateflux_publish_rows_dispatched_total` | counter / rows | `resource` | P0 | 成功发布到队列的行数，按行自身的 `resource` 计数。与 `cron`/`recovery` 批次无关，可直接按资源汇总。 |
| `stateflux_publish_rows_requeued_total` | counter / rows | `resource`, `reason` | P0 | 发布失败后被 `Requeue` 回 pending 的行数。`reason=signature`（组装失败，通常是标签非法或资源未注册）或 `dispatch`（路由/发送失败）。 |
| `stateflux_publish_errors_total` | counter / 1 | `stage` | P0 | 发布通道各阶段错误。`stage` 取值：`create`（落表）、`claim`（认领）、`signature`（组装）、`dispatch`（发布）、`mark_dispatched`（确认）、`ledger`（记账，非阻断）。 |

### 2.3 查询

```promql
# 发布吞吐（行/秒），按资源
sum by (resource) (rate(stateflux_publish_rows_dispatched_total[5m]))

# 发布失败率：以批次为口径
sum(rate(stateflux_publish_batches_total{result="error"}[5m]))
  /
sum(rate(stateflux_publish_batches_total[5m]))

# 认领后未发出的比例（发布通道堵点）
1 - (
  sum(rate(stateflux_publish_rows_dispatched_total[5m]))
  /
  sum(rate(stateflux_publish_rows_claimed_total[5m]))
)

# 重派原因分布
sum by (reason) (rate(stateflux_publish_rows_requeued_total[5m]))

# 批次耗时 P95
histogram_quantile(0.95,
  sum by (le, trigger) (rate(stateflux_publish_batch_seconds_bucket[5m])))
```

### 2.4 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 发布失败 | `rate(stateflux_publish_batches_total{result="error"}[5m]) > 0` | 任一阶段失败；结合 `publish_errors_total{stage}` 定位 |
| 落表失败 | `rate(stateflux_publish_errors_total{stage="create"}[5m]) > 0` | PostgreSQL 不可用或约束冲突 |
| 认领失败 | `rate(stateflux_publish_errors_total{stage="claim"}[5m]) > 0` | 数据库/事务异常 |
| 发布阻塞 | `sum(rate(stateflux_publish_rows_claimed_total[5m])) > 0 and sum(rate(stateflux_publish_rows_dispatched_total[5m])) == 0` | 认领了但一行都发不出去：检查 dm 节点表与 Redis |
| 重派激增 | `sum(rate(stateflux_publish_rows_requeued_total[5m])) > 1` | 路由或队列持续失败，任务在 pending/发布之间循环 |
| 确认失败 | `rate(stateflux_publish_errors_total{stage="mark_dispatched"}[5m]) > 0` | 已发布但阶段未推进，对账会重复收敛 |
