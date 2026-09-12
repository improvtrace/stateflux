# stateflux 设计 · 实施顺序、边界与已确认决策（§12–§14）

> v3.19（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 12. 实施顺序

1. 定义 TaskMessage、ResultEvent、`eventbus/channel` 的 Send/Subscribe 与 capability 契约；先实现
   in-memory fake，作为所有运行时测试基座。
2. 实现 PG identity、四阶段、result、reservation、control lease，以及 fenced claim/terminal/R1 SQL；
   测试丢失或重复 ResultEvent 不破坏终态与名额。
3. 实现 EventBus adapters：unary RPC request/reply、gRPC 双向 ResultStream、Redis list/zset/stream/
   pubsub best-effort adapters。adapter 测试只验证协议行为，不验证 Redis 持久性。
4. 实现 worker：订阅 task、执行 handler、WAL、重连后重发 ResultEvent；实现 Scheduler channel 选择。
5. 实现 Collector result 订阅与批终态；RPC 返回结果必须走同一 ResultEvent 入口。
6. 实现 R1–R5 与故障演练：发送前/后断开、重复消息、所有 Redis 数据删除、ResultStream 断开、双主。
7. 接入 ClusterView、OTel、压测；最后才增加 topic/shard 与新的 adapter。

## 13. 边界

- 不承诺 exactly-once；Redis/RPC 可靠性不属于任务正确性前提。
- 不引入 tenant、多租户权限或 tenant 级配额；idempotency key 由接入方全局命名。
- 不把 Redis ACK/PEL/AOF、RPC 成功返回或 worker 本地状态视为权威。
- 不实现业务创建 RPC、取消、workflow 编排或外部选举服务。

## 14. 已确认决策与遗留项

1. `internal/eventbus/channel` 是唯一节点通信抽象；实现可为 RPC、RPC stream、Redis list/zset/stream/
   pubsub，并标注单向、半双工或全双工能力。
2. 默认同步任务走 unary RPC request/reply；默认异步任务走 Redis channel；二者都以 ResultEvent 归集。
3. 默认结果归集走 worker 与调度节点间的 gRPC 双向 stream，Collector 订阅 EventBus；Redis 结果通道是
   可替换实现。
4. Redis 仅改善通信和削峰，完全丢失时以 PG 对账重跑。
5. v2 候选：外部 Worker 协议、取消/暂停、分片和显式租户模型。
