# stateflux 设计 · 关键机制与 RPC 契约（§6–§7）

> v3.20（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 6. 关键机制

### 6.1 故障恢复矩阵

| 故障 | 处理 |
| --- | --- |
| 任意 Channel send/receive 丢失、重复或超时 | 不推断执行结果；processing 等 grace 后 R1 重置，业务幂等收敛副作用。 |
| Redis 全量丢失或重启 | 不恢复 Redis 状态；按 PG 扫 processing，R1 批量重投。 |
| worker 在 handler/发布结果/WAL 后宕机 | WAL 重放并重发 ResultEvent；WAL 损坏则 R1 重跑。 |
| Collector 终态提交后 channel 返回失败 | 重复 ResultEvent 被 attempt/结果唯一性拒绝；不重复终态或 callback。 |
| 外部选举短暂双主 | 当前 `control_leases` epoch 拒绝陈旧控制写；attempt 拒绝陈旧执行结果。 |

### 6.2 Fence 与对账

attempt fence 覆盖结果、重试、死信和 callback；control epoch 覆盖晋升、claim、R1/R3 与终态控制写。
新 epoch 可归集旧 epoch claim 的有效结果，但必须验证 task attempt，不能要求任务行记录的历史 epoch 等于
当前 epoch。

- **R1**：processing 超过 `max(timeout, dispatch_grace) + skew` 且未终态，释放名额并移回
  schedulable，写 full-jitter 退避；不增加 attempts。
- **R2**：清理可选 Redis 容量提示视图（`domain/cacheview`，§8）；不据此修改 PG。
- **R3**：重试时若 `attempts >= max_attempts`，事务内释放名额并写 `completed{dead}`。
- **R4**：Redis 或所有 channel 不可用后的 PG 扫描重投；没有“恢复消息队列”的正确性步骤。
- **R5**：由 processing 聚合重算并发 reservation，修复异常退出造成的漂移。

### 6.3 Channel 能力与运行规则

EventBus 的可靠性契约固定为 best-effort；实现不能把自己的能力提升为架构承诺。RPC unary 是半双工
request/reply；gRPC stream 可全双工发送任务/结果；Redis list/zset/stream 是异步单向队列；Redis
pub/sub 是异步广播。所有实现须：携带 task_id/attempt/correlation_id、支持 context 取消、暴露连接和
投递错误指标，并允许订阅端重新连接。只要实现遵守这些语义，替换不影响生命周期。

默认部署使用 RPC channel 传同步任务、gRPC 双向 stream 传 Worker ResultEvent；Redis channel 适合削峰
或解耦，但不是可靠消息系统。backpressure 用 scheduler claim credit、RPC/stream 流控、worker WAL
水位和 PG pending 告警完成，不以 Redis backlog 作为唯一信号。

### 6.4 观测

OpenTelemetry 指标统一带 `stateflux.` 前缀：`stateflux.eventbus.send`、`stateflux.eventbus.subscribe`、
`stateflux.eventbus.errors`（channel/kind 标签）、`stateflux.collector.results`、
`stateflux.reconcile.resets`、`stateflux.control.fence_rejects`、`stateflux.wal.backlog.entries` /
`.bytes`、`stateflux.handler.duration` 与 `stateflux.concurrency.reservations`。禁止
task_id/idempotency_key 作为标签。水位型指标（wal.backlog、concurrency.reservations）用同步
Int64Gauge，采集点收敛在既有写路径，免去回调装配。Redis 指标只用于诊断，不作为 SLO 的正确性来源。

## 7. RPC 契约

- `TaskMessage` 与 `ResultEvent` 是 EventBus 统一载荷；RPC/stream adapter 只负责编解码。
- Unary `Execute` 是 request/reply channel：返回 ResultEvent；Scheduler 将其再送入 result topic。
- gRPC bidirectional `ResultStream` 是默认全双工结果 channel：worker publish，Collector subscribe；
  断线重连后从 worker WAL 重发。
- Redis list/zset/stream/pubsub adapters 仅实现相同的 Send/Subscribe 能力；不把 XACK、PEL 或持久化
  暴露为 domain 语义。
