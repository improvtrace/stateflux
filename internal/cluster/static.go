package cluster

import (
	"context"

	"github.com/improvtrace/stateflux/internal/config"
)

// DefaultLocalNode 是 local://（单节点部署）的内置节点身份：primary（数据库读写，
// 全部服务权限），监听默认 gRPC 地址。节点信息不来自任何配置文件。
var DefaultLocalNode = Node{
	ID:          "local",
	Address:     "127.0.0.1:9090",
	Labels:      []string{},
	Roles:       []string{RolePrimary},
	Permissions: []NodePermission{NodePermissionOperation, NodePermissionExecute, NodePermissionSchedule, NodePermissionCollect},
	Online:      true,
	IsLeader:    true,
}

// staticView 是 local://（单节点部署）实现（§2.1）：节点清单与 leader 位置直接来自
// 本进程配置，不访问任何外部系统。一个进程闭环。
type staticView struct {
	info Info
}

// NewStaticView 构造单节点视图：节点信息取内置 DefaultLocalNode（不读配置），
// 本节点在线且即 leader，使默认配置可直接单机运行。DSN query 仍可覆盖 node_id /
// address；timeout / interval 等参数无外部连接可约束，仅保留解析容错。
func NewStaticView(opts config.ClusterOptions) View {
	node := DefaultLocalNode
	if v := opts.Param("node_id"); v != "" {
		node.ID = v
	}
	if v := opts.Param("address"); v != "" {
		node.Address = v
	}
	return &staticView{info: Info{
		Nodes:           []Node{node},
		NodeID:          node.ID,
		SchedulerNodeID: node.ID,
	}}
}

// Get 实现 View：静态视图永远返回同一份快照。
func (v *staticView) Get(context.Context) (Info, error) { return v.info, nil }
