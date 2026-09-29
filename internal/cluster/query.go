package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/improvtrace/stateflux/internal/config"
)

// Query 按集群视图 DSN 拉取一次完整快照（§15.1#3）：local:// 走本地静态视图，
// grpc:// 与 http(s):// 走对应客户端——与 serve 的消费路径（New 分发）完全一致，
// 供 info 子命令等只读查询使用。DSN query 里的 timeout / connect_timeout 照常生效。
func Query(ctx context.Context, dsn string) (Info, error) {
	view, err := New(config.Cluster{DSN: dsn})
	if err != nil {
		return Info{}, err
	}
	if c, ok := view.(interface{ Close() error }); ok {
		defer c.Close()
	}
	return view.Get(ctx)
}

// Describe 把一份视图快照格式化为多行可读文本（stateflux info 的输出）：上报
// 身份、调度节点与全部节点明细（含客户端按角色推导的服务权限）。
func Describe(i Info) string {
	var b strings.Builder
	nodeID := i.NodeID
	if nodeID == "" {
		nodeID = "(unset)"
	}
	scheduler := i.SchedulerNodeID
	if scheduler == "" {
		scheduler = "(none)"
	}
	fmt.Fprintf(&b, "node_id:   %s\n", nodeID)
	fmt.Fprintf(&b, "scheduler: %s\n", scheduler)
	fmt.Fprintf(&b, "nodes:     %d\n", len(i.Nodes))
	for _, n := range i.Nodes {
		fmt.Fprintf(&b, "  %-16s address=%s vpc=%s labels=%v roles=%v perms=%v online=%t leader=%t\n",
			n.ID, n.Address, n.VPC, n.Labels, n.Roles, n.Permissions, n.Online, n.IsLeader)
	}
	return b.String()
}
