package cluster

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestQueryLocal 覆盖 Query 的 local:// 路径：空 DSN 与显式 local 均返回内置
// 静态单节点视图（serve 默认配置所见）。
func TestQueryLocal(t *testing.T) {
	for _, dsn := range []string{"", "local://localhost"} {
		info, err := Query(context.Background(), dsn)
		if err != nil {
			t.Fatalf("Query(%q): %v", dsn, err)
		}
		if len(info.Nodes) != 1 || !info.Nodes[0].IsLeader || info.Nodes[0].ID != DefaultLocalNode.ID {
			t.Fatalf("Query(%q) = %+v, want default local leader node", dsn, info)
		}
		if info.NodeID != DefaultLocalNode.ID || info.SchedulerNodeID != DefaultLocalNode.ID {
			t.Fatalf("Query(%q) identity = %q/%q, want %q", dsn, info.NodeID, info.SchedulerNodeID, DefaultLocalNode.ID)
		}
	}
}

// TestQueryErrors 覆盖非法 DSN 与不可达目标的报错。
func TestQueryErrors(t *testing.T) {
	if _, err := Query(context.Background(), "ftp://host"); err == nil || !strings.Contains(err.Error(), "unknown scheme") {
		t.Fatalf("Query(ftp://) error = %v, want unknown scheme", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Query(ctx, "grpc://127.0.0.1:1?timeout=300ms"); err == nil {
		t.Fatal("Query to unreachable grpc addr should fail")
	}
}

// TestDescribe 覆盖视图的全量可读输出：身份、调度节点与节点明细（含推导权限）。
func TestDescribe(t *testing.T) {
	info := Info{
		NodeID:          "node-1",
		SchedulerNodeID: "node-1",
		Nodes: []Node{
			{ID: "node-1", Address: "127.0.0.1:9090", VPC: "vpc-a", Labels: []string{"zone=a"},
				Roles: []string{RolePrimary}, Online: true, IsLeader: true,
				Permissions: permissionsOf([]string{RolePrimary})},
			{ID: "node-2", Address: "127.0.0.1:9092", Roles: []string{RoleStandby}, Online: true,
				Permissions: permissionsOf([]string{RoleStandby})},
		},
	}
	text := Describe(info)
	for _, want := range []string{
		"node_id:   node-1",
		"scheduler: node-1",
		"nodes:     2",
		"node-1",
		"address=127.0.0.1:9090",
		"vpc=vpc-a",
		"labels=[zone=a]",
		"roles=[primary]",
		"perms=[operation execute schedule collect]",
		"online=true",
		"leader=true",
		"node-2",
		"perms=[operation execute]",
		"leader=false",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Describe output missing %q:\n%s", want, text)
		}
	}

	// 空身份/无调度节点的兜底展示。
	empty := Describe(Info{})
	for _, want := range []string{"node_id:   (unset)", "scheduler: (none)", "nodes:     0"} {
		if !strings.Contains(empty, want) {
			t.Fatalf("Describe(empty) missing %q:\n%s", want, empty)
		}
	}
}
