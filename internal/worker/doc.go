// Package worker 是执行侧运行时（常驻所有节点）：消费循环、租约续期、结果 WAL
// 与 api/stateflux/task/v1 gRPC server；业务实现由 biz 提供，经 server 装配注入，
// 本包不 import biz。
package worker
