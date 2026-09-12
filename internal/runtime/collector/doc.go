// Package collector 实现阶段 4 结果归集：订阅 EventBus 的 result topic（同步 RPC 的响应
// 适配为同一 ResultEvent 后同样从该入口进入），每个事件经一次 PG 事务完成终态搬移
// （payload 合并、task_results 写入、回调派生）并写墓碑；重复事件无副作用（§5.5）。
// 仅调度节点运行。
package collector
