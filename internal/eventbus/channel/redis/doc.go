// Package redis 是 eventbus/channel 的实现子目录：在 Redis 上的 best-effort 实现（§3.2、§12.3）：
// list、zset、stream、pubsub 四种形态都只是异步单向投递的通道，等价于一次不等结果的调用。
//
// 实现纪律（§9.2、§9.3、§13）：
//
//   - 不把 XACK、PEL、consumer group、AOF 或任何 Redis 状态暴露为 domain 语义，
//     框架的正确性不依赖它们；
//   - 不依赖 Redis 持久化保证投递：Redis 全量丢失时由 PG 扫描 processing 经 R1 重投（R4），
//     本包不提供「恢复消息队列」的动作；
//   - 不把 Redis backlog 当作唯一反压信号（§6.3）；
//   - 指标只用于诊断，不作为 SLO 的正确性来源（§6.4）。
//
// 通道与任务是可替换关系：任务行只记录逻辑 channel 名，由装配决定它落在 list、zset、
// stream 还是 pubsub 上；换实现不改变状态机。
package redis
