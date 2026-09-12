# stateflux

基于 **PostgreSQL（唯一权威）+ 可替换 EventBus 通道** 的分布式任务调度服务。任务可靠性不依赖
Redis、RPC 或任何消息通道的持久性：PG 的四阶段账本、attempt fencing 和对账负责收敛；业务 handler
仍必须幂等。

当前设计为 **v3.19**，详见 [设计索引](./.docs/design/README.md)。仓库处于骨架阶段，按设计 §12 实施。

## 核心设计

- **四阶段账本**：`pending → schedulable → processing → completed`；结果不可变，PG 是唯一事实来源。
- **事务入队**：业务在自己的 PG 事务中调用 `enqueue_tasks`，以全局唯一 `idempotency_key` 获得稳定 task ID；
  本期不引入 tenant。
- **fenced 调度**：PG control epoch、原子并发 reservation 与 attempt 只在 claim 时递增，抵御双主和陈旧结果。
- **统一 EventBus**：`internal/eventbus/channel` 提供 Send/Subscribe，以及单向、半双工 request/reply 和
  全双工 stream 能力；RPC、gRPC stream、Redis list/zset/stream/pubsub 都是 adapter。
- **同步与异步仅是 channel 差异**：同步 RPC 立即返回 ResultEvent；异步 Redis 只尝试发送，worker 之后
  发布 ResultEvent。Collector 统一订阅结果集并终态化。
- **Redis 不可靠**：Redis 全丢、消息丢失或重复、RPC 超时都会由 processing grace + PG R1 对账重跑。
- **结果归集**：默认 worker→调度节点为 gRPC 双向 ResultStream；worker WAL 支持断线重连后的结果重发。

## 布局

```text
internal/
├── runtime/          # scheduler / collector / reconcile（仅调度节点运行）
├── worker/           # 订阅任务、执行 handler、结果 WAL、发布 ResultEvent
├── eventbus/
│   ├── channel/      # Send/Subscribe 与单向、半双工、全双工能力 + in-memory fake
│   ├── rpc/          # unary Execute / gRPC 双向 ResultStream adapter
│   └── redis/        # list / zset / stream / pubsub best-effort adapter
├── task/             # factory（周期任务）+ dispatch（调度侧分发与 ResultSink）
├── domain/           # 任务模型、PG 聚合接口、identity / fence / reservation
└── server/           # 装配
```

## 开发

```bash
make proto
make generate
make build
```

本地中间件：`hack/dev.sh up`。生产 Redis 仅作为通道部署；其 AOF、复制、ACK 或 Stream PEL 不能成为
stateflux 正确性假设。
