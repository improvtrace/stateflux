// Package collector 实现阶段 4 结果归集：Collect 拉取 → 终态搬移（payload 合并、
// task_results 写入、回调派生同一事务）→ 墓碑 → Ack。仅调度节点运行。
package collector
