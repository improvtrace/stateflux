// Package eventbus 是任务与结果的统一通信面（§3.2）：EventBus 实例管理一组 channel
// （创建时注册、运行期可增删与解析），并面向节点提供事件订阅——订阅的目标是节点，
// 一个节点有 system 与 result 两类事件（topic：node.{nodeID}.{event}）；任务分发仍走
// task.{band} topic（§5.3）。传输由 internal/eventbus/channel 的实现承载——实现按子目录
// 组织：channel/mem（in-memory fake）、channel/rpc（unary 与 gRPC stream adapter）、
// channel/redis（list/zset/stream/pubsub）。
//
// 可靠性契约固定为 best-effort：丢消息、重复、乱序，以及「返回发送失败但实际已送达」都在
// 契约之内。调用者不得从 Call/Send 的结果推断任务是否被执行，任务正确性只由 PG 的认领、
// 终态与对账（R1–R4）保证（§1.2.1、§4）。因此本包与所有实现都不提供持久化、至少一次或
// 恰一次承诺，也不把任何 broker 的 ack/AOF/PEL/consumer group 提升为架构承诺（§9.3）。
//
// topic 约定（§3.2）：任务为 task.{band}（TaskTopic）——band 是数值 priority 的分档
// （low/normal/high，见 schema.BandOf），不是原始 0–100 数值；节点事件为
// node.{nodeID}.{event}（NodeTopic），event ∈ {system, result}。使用方式：
//
//   - 装配期：NewEventBus 注册全部 channel 实现（逻辑 channel 名 → 实现）；
//   - 调度侧：按任务行的 channel 名经 Resolve 解析实现（或 EventBus.Call/Send）分发，
//     且只 claim 当前可立即发送的量（§5.3）；
//   - 执行侧：worker 发布 ResultEvent 到自己的 result 节点 topic；
//   - 归集侧：Collector 用 EventBus.Subscribe 订阅各节点的 result 事件；同步 RPC 的响应
//     适配为同一个 ResultEvent 后同样走此处（§5.5）。
//
// 双工形态由实现的 Capabilities 推导（单向 / 半双工 request/reply / 全双工 stream），
// 调用方据此决定是否等待结果——同步与异步的差别仅此而已，不是两套机制（§14.1）。
package eventbus
