// Package worker 是执行侧运行时（常驻所有节点）：订阅任务 channel、执行 handler、先把结果
// 写入本地 WAL，再发布 ResultEvent（默认结果通道是到调度节点的 gRPC 双向 ResultStream，
// 断线重连后重发未确认条目，§5.4）。worker 不写 PG 终态、不决定重试，本地 WAL 与容量上报
// 都是易失的派生数据。业务实现由 biz 提供、经 server 装配注入，本包不 import biz。
package worker
