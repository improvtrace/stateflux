# stateflux 设计 · 关键机制与 RPC 契约（§6–§7）

> v3.16（2026-09-10）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 6. 关键机制

### 6.1 故障恢复矩阵

| 故障 | 后果 | 收敛机制 |
| --- | --- | --- |
| 调度节点宕机 | 调度暂停；已认领未投递的任务滞留 processing | 外部选举切换；对账重置滞留任务 |
| 执行节点宕机（执行中） | 租约到期不再续 | 对账重置 → 重新认领重跑 |
| 执行节点宕机（结果未归集） | WAL 已落盘，任务滞留 processing | 重启重放 WAL 继续供拉；WAL 损坏则 R1 重跑兜底（幂等收敛） |
| Collect Ack 丢失 | WAL 保留，条目仍占 inprocess | 重拉 → PG 幂等回写 → 重新 Ack |
| 陈旧队列副本（重试后旧消息残留） | 两个执行节点先后消费同一任务 | 注册被拒（成员已存在/墓碑命中）→ 无害丢弃 |
| Redis 全量丢失 | 队列与 inprocess 清空 | 对账从 PG 重建：processing 全部按 R1 重置 |
| PG 单点 | 全框架不可用 | 中心化部署 + 云 HA / Patroni，超出框架范围 |

### 6.2 陈旧副本与墓碑

重试会重新入队，旧副本可能仍躺在队列里。两道防线：注册时的 attempt 校验（同一 attempt 成员已存在
→ 拒绝）；终态墓碑（任务已落终态后旧副本才被消费 → 拒绝）。墓碑 TTL ≥ grace（默认 90s），自动
回收。TTL 必须覆盖 grace 而非仅覆盖对账周期：若旧副本在「墓碑已过期、且新尝试已终态移出
inprocess」之后才被消费，两道防线均失效，旧 attempt 将重复执行——结果会被 attempt 校验挡住不落库，
但副作用已发生；TTL 取 ≥ grace 是廉价保险。

### 6.3 对账规则（运行于调度节点，周期 30s）

以 PG 为准绳、inprocess 为实时视图，双向核对：

- **R1 重置**：`processing_tasks` 中 `updated_at` 超过 grace（默认 90s），且不在 inprocess 或
  租约已过期 → 挪回 `schedulable_tasks`（attempt+1，`run_at = now + 退避`；约束已通过，不重评）。
  退避实现为 **capped exponential backoff + full jitter**（AWS 模式；默认初值 500ms、上限 1000s，
  参照 client-go workqueue）——无 jitter 的重试会在故障恢复后形成同步重试风暴，正好打在刚恢复的
  调度节点上；
- **R2 幽灵清理**：inprocess 中存在但 PG 已终态/不存在的条目 → 强制移除；
- **R3 死信**：重置时 `attempts >= max_attempts` → 直接挪入 `completed_tasks{dead}`；
- **R4 重建**：Redis 丢失后，扫全部 `processing_tasks` 按 R1 重置并重建集合结构。

### 6.4 反压

- 调度侧：claim 数由下游容量（队列深度、闸门余量）实时决定，从源头不超发；
- 执行侧：BRPOP 自节奏消费，处理不过来时消息滞留队列，深度反馈给调度侧缩 claim；
- 创建侧：不设反压，PG 吸收。

### 6.5 观测：OpenTelemetry 指标与链路追踪

- **单一采集标准**：全框架指标与链路追踪统一走 OpenTelemetry（`go.opentelemetry.io/otel/`）。
  `obs/` 装配全局 MeterProvider 与 TracerProvider（metrics + tracing，OTLP 导出，默认 60s 推送，
  endpoint/协议可配，装配参数来自 `config/`），各角色包只依赖注入的 Meter/Tracer，不感知导出协议；
  两者共用同一 OTel SDK 与 Resource（service.name、node_id、版本/环境标签），跨进程传播由
  TaskMessage 的 `trace_headers`（§3.3）承载 W3C 传播头。
- **采集点位收敛在既有写路径**：晋升、认领、终态事务、对账扫描、注册拒绝、WAL 水位等指标在
  repository/cacheview/角色包的既定操作内埋点，不引入独立采集组件；gRPC 侧用统一 interceptor（unary/
  stream）产出 rpc qps/错误率/延迟。
- **最小指标集**（§12 压测调优的观测对象）：

| 指标 | OTel 类型 | 标签 | 归属 / 含义 |
| --- | --- | --- | --- |
| `stateflux.queue.depth` | ObservableGauge | priority | 调度：各就绪队列深度（LLEN）|
| `stateflux.inprocess.size` | ObservableGauge | — | 调度/对账：共识集合大小（ZCARD）|
| `stateflux.claim.to_deliver_latency` | Histogram | — | 调度：claim→投递延迟 |
| `stateflux.collect.latency` | Histogram | — | Collector：结果完成→终态落库（归集延迟）|
| `stateflux.reconcile.resets` | Counter | reason | 对账：R1 重置次数（核心健康信号）|
| `stateflux.inprocess.rejects` | Counter | kind（tombstone/attempt）| cacheview：注册拒绝数（§6.2 双防线命中）|
| `stateflux.tasks.terminal` | Counter | outcome | Collector：终态计数（succeeded/failed/dead）|
| `stateflux.wal.backlog` | ObservableGauge | — | Worker：WAL 未 Ack 条数/字节数（两个实例，反压水位）|
| `stateflux.executor.free_slots` | ObservableGauge | node_id | Worker：容量上报 |
| `stateflux.handler.duration` | Histogram | type | Worker：handler 耗时 |

- **标签纪律**：只允许低基数标签（priority、type 白名单、node_id、outcome、reason）；task_id/
  业务 key 禁止入标签，超限枚举折叠为 `other`，防标签基数爆炸。

## 7. RPC 契约

- **内部 gRPC（调度角色 ↔ 执行角色）**：`Execute`（同步任务，带 deadline）与 `Collect`（结果归集，
  pull + Ack）。
- **消息载体**：`api/stateflux/task/v1`（task.proto，原 dispatch.proto）定义统一 `TaskMessage`
  信封（§3.3），异步队列消息与 `Execute` 请求共用。
- **外部契约（外部选举/成员系统实现，stateflux 只消费）**：`GetClusterInfo` 返回节点列表
  （ID/地址/角色/能力标签）与当前调度节点 ID。
- 执行接入点（§7）：业务逻辑以 `Handler`（返回任务类型 + 执行函数）实现，在服务构建时注册
  （内置服务装配点 = `internal/server.RegisterHandlers`；demo Handler 提供冒烟），服务负责生命周期
  与超时注入；`api/stateflux/task/v1` 的服务端业务（Execute/Collect 编排）由 `internal/biz` 承载，
  worker 的 gRPC server 经 server 装配委托 biz（§8）；关键接口的 Go 定义与 proto 细节在实施阶段给出。
