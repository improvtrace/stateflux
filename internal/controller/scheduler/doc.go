// Package scheduler 是调度核心：约束晋升预处理、自适应认领、同步分发池、异步投递与
// 选节点。随外部选举（ClusterView）启停；与 collector/reconcile 同属控制面
// （internal/controller）。
package scheduler
