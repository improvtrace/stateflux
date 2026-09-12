// Package scheduler 是调度核心：约束晋升预处理、fenced 原子名额认领、按任务 channel 选择
// eventbus/channel 实现并分发、选执行节点；且只 claim 当前可立即发送的量（§5.3）。通道错误
// 不推断执行结果，路由与并发约束全部留在调度侧（§1.2.7）。随外部选举（ClusterView）与
// PG control epoch 启停；与 collector/reconcile 同属控制面（internal/runtime）。
package scheduler
