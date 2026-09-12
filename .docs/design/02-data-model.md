# stateflux 设计 · 数据模型与状态机（§3–§4）

> v3.19（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 3. 数据模型

### 3.1 PG 任务账本

四阶段 Pending / Schedulable / Processing / Completed 是 `internal/domain` 暴露的逻辑集合，PG 四张
阶段表为默认实现。业务在自己的本地事务内调用 `stateflux.enqueue_tasks(...)`；业务 role 仅有该函数
的 EXECUTE 权限与结果查询权限，内部表只由服务账号写入。

任务行包含 `id`、`type`、`priority`、`channel`、`run_at`、`timeout_ms`、`max_attempts/attempts`、
`attempt`、`owner_node`、`control_epoch`、`idempotency_key`、`callback`、`parent_task_id` 与时间戳。
项目暂不引入 tenant：`idempotency_key` 在整个 stateflux 实例内唯一，接入方必须使用全局唯一前缀。

| 表 | 用途 |
| --- | --- |
| `task_identities` | `idempotency_key → task_id` 的跨阶段身份账本；dedupe 窗口内重复创建返回原 ID。 |
| `task_payloads` | 在途 payload；终态时合并入 completed。 |
| `pending_tasks` / `schedulable_tasks` / `processing_tasks` / `completed_tasks` | 四阶段集合。 |
| `task_results` | `task_id` 主键的不可变终态结果。 |
| `concurrency_reservations` | `scope_key` 的在途计数与上限；claim 时条件预留，终态/重置时释放。 |
| `control_leases` | 控制面 scope 的 owner、expiry、单调 epoch；外部选举只提供候选资格。 |

claim 是一个 PG 事务：验证当前 control epoch → `SKIP LOCKED` 选候选 → 条件预留名额 →
`schedulable → processing` → `attempts + 1`。attempt 是执行 fence，**只在 claim 加一**；重试/R1
重置不加。终态、重置和死信验证 attempt，终态操作验证当前 control lease。前置条件只能读取不变任务
字段或受限快照，不得 I/O 或写状态。

### 3.2 EventBus 与 Channel

`internal/eventbus` 是任务和结果的统一通信面：`task.{priority}` 与 `result` 是逻辑 topic；它支持
`Send`（发布）和 `Subscribe`（订阅）。EventBus 不承诺持久化、至少一次或恰一次；调用者必须接受
**丢失、重复、乱序，以及“返回发送失败但实际已送达”**。PG processing + timeout/grace + 对账是唯一
恢复机制。

`internal/eventbus/channel` 定义能力而非绑定中间件：

| channel 能力 | 语义 | 实现例子 |
| --- | --- | --- |
| 单向 publish/subscribe | 发送不带结果；订阅端随后发布 ResultEvent | Redis pub/sub、list、zset、stream |
| 半双工 request/reply | 先发送请求、再读取一个结果；调用方阻塞等待 | unary RPC、Redis list 组合 |
| 全双工 stream | 两端可并发 Send/Subscribe；适合长连接结果汇聚 | gRPC bidirectional stream、Redis pub/sub |

每个实现声明 `ChannelCapabilities`（是否订阅、是否请求应答、是否全双工、是否提供本地 ack）。ack 是
实现级流控信号，**永不构成任务可靠性条件**。Redis 的 list/zset/stream/pubsub 可全部实现 channel；即使
某实现有 AOF、PEL 或 XACK，框架也不依赖它们。默认结果 channel 是调度节点和执行节点之间的 gRPC
双向 stream：worker 发布 `ResultEvent`，Collector 订阅；它可替换成 Redis channel 而不改变状态机。

### 3.3 信封

`api/stateflux/task/v1` 定义不可变 `TaskMessage`（task_id、attempt、type、payload、priority、deadline、
trace_headers、control_epoch）和 `ResultEvent`（task_id、attempt、outcome、result/error、source）。
同步 RPC channel 的响应适配为 `ResultEvent` 并送入同一 EventBus；异步 channel 的 `Send` 只表示已尝试
发送，结果必须由执行节点另行发布。大 payload/result 使用对象存储引用。

## 4. 状态机与可靠性

```
pending → schedulable → processing ──ResultEvent（attempt 匹配）→ completed
               ▲              │
               └─ retry / R1 ─┘  （不加 attempts；下次 claim 才 +1）
```

Channel 故障、Redis 丢失、RPC 超时或结果事件丢失都只会让 processing 在 grace 后重置并重跑；业务 handler
必须按 `idempotency_key` 或 task_id 保证副作用幂等。Redis 仅缩短正常路径延迟，完全不可用时框架仍由
PG 对账收敛。
