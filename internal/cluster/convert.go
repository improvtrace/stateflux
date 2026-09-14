package cluster

import (
	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
)

// fromProto 把外部集群视图的 NodeInfo 转为领域节点（§15.1#3）。
func fromProto(n *clusterv1.NodeInfo) Node {
	if n == nil {
		return Node{}
	}
	return Node{
		ID:           n.GetNodeId(),
		Address:      n.GetAddress(),
		VPC:          n.GetVpc(),
		Label:        n.GetLabel(),
		Roles:        append([]string(nil), n.GetRoles()...),
		Capabilities: append([]string(nil), n.GetCapabilities()...),
	}
}

func infoFromProto(resp *clusterv1.ClusterInfoResponse) Info {
	if resp == nil {
		return Info{}
	}
	info := Info{SchedulerNodeID: resp.GetSchedulerNodeId()}
	for _, n := range resp.GetNodes() {
		info.Nodes = append(info.Nodes, fromProto(n))
	}
	return info
}

// nodeFromConfig 把配置里的节点身份转为领域节点（static 模式）。
func nodeFromConfig(n config.Node) Node {
	return Node{
		ID:           n.NodeID,
		Address:      n.Address,
		VPC:          n.VPC,
		Label:        n.Label,
		Roles:        append([]string(nil), n.Roles...),
		Capabilities: append([]string(nil), n.Capabilities...),
	}
}
