package cluster

import (
	"context"

	"github.com/improvtrace/stateflux/internal/config"
)

// staticView 是单机/开发模式实现（§2.1）：节点清单与调度节点位置直接来自配置，
// 不访问任何外部系统。所有角色可赋给本进程，一个进程闭环。
type staticView struct {
	info Info
}

// NewStaticView 用 config.Cluster.StaticNodes 构造静态视图；清单为空时退化为
// 「本节点 + 本节点即调度节点」，使默认配置可直接单机运行。
func NewStaticView(cfg config.Cluster, local config.Node) View {
	nodes := make([]Node, 0, len(cfg.StaticNodes))
	for _, n := range cfg.StaticNodes {
		nodes = append(nodes, nodeFromConfig(n))
	}
	if len(nodes) == 0 {
		nodes = append(nodes, nodeFromConfig(local))
	}
	scheduler := cfg.StaticSchedulerNodeID
	if scheduler == "" {
		if local.NodeID != "" {
			scheduler = local.NodeID
		} else {
			scheduler = nodes[0].ID
		}
	}
	return &staticView{info: Info{Nodes: nodes, SchedulerNodeID: scheduler}}
}

// Get 实现 View：静态视图永远返回同一份快照。
func (v *staticView) Get(context.Context) (Info, error) { return v.info, nil }
