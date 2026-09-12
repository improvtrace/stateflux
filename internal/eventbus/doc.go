// Package eventbus 是任务与结果的统一通信面（§3.2）：逻辑 topic 上的 Send（发布）与
// Subscribe（订阅）。传输由 internal/eventbus/channel 的实现承载——RPC、gRPC stream 与
// Redis list/zset/stream/pubsub 都只是它的不同实现。
//
// 可靠性契约固定为 best-effort：丢消息、重复、乱序，以及「返回发送失败但实际已送达」都在
// 契约之内。调用者不得从 Send/Call 的结果推断任务是否被执行，任务正确性只由 PG 的认领、
// 终态与对账（R1–R5）保证（§1.2.1、§4）。因此本包与所有实现都不提供持久化、至少一次或
// 恰一次承诺，也不把任何 broker 的 ack/AOF/PEL/consumer group 提升为架构承诺（§9.3）。
//
// topic 约定（§3.2）：任务为 task.{priority}（TaskTopic），结果为 result（ResultTopic）。
// 使用方式：
//
//   - 调度侧：按任务行的 channel 名经 Registry 解析实现，用 Bus.Send 分发，且只 claim
//     当前可立即发送的量（§5.3）；
//   - 执行侧：worker 用 Bus.Subscribe 订阅任务 topic，执行后把 ResultEvent 发到结果 topic；
//   - 归集侧：Collector 用 Bus.Subscribe 订阅 ResultTopic；同步 RPC 的响应适配为同一个
//     ResultEvent 后同样走此处（§5.5）。
//
// 双工形态由实现的 Capabilities 推导（单向 / 半双工 request/reply / 全双工 stream），
// 调用方据此决定是否等待结果——同步与异步的差别仅此而已，不是两套机制（§14.1）。
package eventbus
