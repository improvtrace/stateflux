// Package rpc 是 eventbus/channel 的实现子目录（§6.3、§7）：承载 api/stateflux/task/v1 的
// gRPC adapter，把两种 wire 形态适配为 channel 的两种能力；本包只做协议编解码，不解释任务语义：
//
//   - unary Execute：半双工 request/reply（channel.Requester）。调度侧发送 TaskMessage 并
//     阻塞等待一个结果，响应适配为 ResultEvent 后送入同一个 result topic（§5.5）；
//   - 双向 ResultStream：全双工（channel.Requester + channel.Subscriber）。worker 发布
//     ResultEvent、Collector 订阅；stream 断开后 worker 重连并由本地 WAL 重发未确认结果（§5.4）。
//
// 纪律：RPC 成功返回不表示任务已完成、失败也不表示任务未执行——两个方向的推断都是错的，
// 收敛一律交给 PG 对账（§5.3）。实施顺序见 §12.3。
package rpc
