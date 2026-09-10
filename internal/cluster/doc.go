// Package cluster 提供集群视图（ClusterView）外部接口、RPC 客户端与 static 单机
// 实现。集群信息只读不治：选举由外部系统负责，stateflux 仅消费。
package cluster
