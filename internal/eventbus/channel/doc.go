// Package channel 定义节点通信的能力抽象（§3.2），是 internal/eventbus 唯一的传输契约。
//
// 三种能力（由 ChannelCapabilities 声明，而不是由中间件类型决定）：
//
//   - 单向 publish/subscribe：发送不带结果，订阅端之后另行发布 ResultEvent
//     （Redis pub/sub、list、zset、stream）；
//   - 半双工 request/reply：先发送请求、再读取一个结果，调用方阻塞等待（unary RPC）；
//   - 全双工 stream：两端可并发发送与订阅，适合长连接结果汇聚（gRPC 双向 stream、
//     Redis pub/sub）。
//
// 同步调用与异步调用不是两套机制，而只是上述能力的不同实现：同步 RPC 立即返回结果，
// 异步实现只确认「已尝试发送」，结果由执行节点之后发布（§14.1）。实现可以声明 LocalAck
// 表示自己暴露了投递确认信号（XACK、RPC 返回成功等），它仅用于流控与诊断，**永不构成
// 任务可靠性条件**（§3.2、§9.3）。
//
// 每个实现必须做到（§6.3）：信封携带 task_id/attempt/correlation_id、尊重 context 取消、
// 暴露连接与投递错误指标、允许订阅端重新连接。实现的替换不得改变状态机——丢失、重复、
// 乱序与发送结果不确定都被允许，收敛一律由 PG 对账（R1–R5）完成。
package channel
