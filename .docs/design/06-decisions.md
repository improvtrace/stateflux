# stateflux 设计 · 关键决策、参数与扩展路径（§9–§11）

> v3.20（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 9. 关键设计决策

1. PG 是任务可靠性的唯一基础；Redis 不提供持久化保证，也不参与正确性判断。
2. `eventbus/channel` 统一 RPC、gRPC stream 与 Redis 的任务/结果通信；同步 RPC 与异步 Redis 只是
   request/reply 和 publish/subscribe 两种 channel 能力。
3. EventBus 只定义 Send/Subscribe、统一信封与能力声明；不把具体 broker 的 ack、PEL、消费组或
   连接模型泄漏进 domain。
4. Collector 订阅 `result` topic；默认 Worker→调度节点为 gRPC 双向 ResultStream，但可替换为任何
   Redis channel。同步 Execute 的响应也转换为 ResultEvent。
5. `task_identities(idempotency_key)` 提供全实例幂等；本期不引入 tenant，接入方负责键的全局唯一性。
6. control epoch、并发 reservation、attempt fence 保持在 PG；attempt 只在 claim 加一。
7. channel 失败统一由 R1 重置 processing 并重试，故业务 handler 必须幂等。

## 10. 默认参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| claim batch / tick | 500 / 100ms | 受 worker credit 与并发 reservation 收缩。 |
| promotion tick | 200ms | NOTIFY 仅是唤醒优化。 |
| direct RPC pool | 512 | request/reply channel 并发上限。 |
| dispatch grace | 90s | 通道无结果前不重试的最小窗口。 |
| 执行预算 `timeout_ms` | 60s | 与任务行默认一致（§3.1）。worker 超时必须中断并发布结果；R1 兜底阈值 `max(timeout_ms, dispatch_grace) + skew` 同时是该值的重置延迟上界（§6.2），故它不是「必须小于 grace」的硬约束。 |
| ResultStream reconnect | 1s → 30s | WAL 未确认结果持续重发。 |
| WAL 高水位 | 10k 条或 256MB | 暂停新订阅、继续结果发送。 |
| retry backoff | 500ms → 1000s | capped exponential + full jitter。 |

5k task/s 仍是压测目标，口径为**单调度节点**（claim、晋升与结果归集同在该节点，§11）。不同 Redis
channel 的吞吐/持久特性只影响延迟、成本和恢复速度，不能改变正确性指标；压测必须包含断开 RPC
stream、丢弃 Redis 全部数据、重复 ResultEvent 与 Send 不确定结果。
表中 `skew` 以及 worker credit、dedupe 窗口、对账周期等尚未定稿的参数见 §14.6–§14.13。

## 11. 扩展路径

先增加 channel adapter，不变更状态机；再按 type 拆 task/result topic；最后才引入 control shard。
每个 shard 都有显式 PG control lease 与独立结果订阅，不通过 Redis 分区状态确定 ownership。
