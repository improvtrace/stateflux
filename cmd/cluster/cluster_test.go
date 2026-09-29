package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/improvtrace/stateflux/internal/cluster/mockserver"
)

func TestClusterCommandDefaults(t *testing.T) {
	var got mockserver.Config
	cmd := NewClusterCommand(ClusterDeps{Runner: func(_ context.Context, cfg mockserver.Config) error {
		got = cfg
		return nil
	}})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := mockserver.DefaultNodes()
	if got.GRPCAddr != mockserver.DefaultGRPCAddr || got.HTTPAddr != mockserver.DefaultHTTPAddr {
		t.Fatalf("addrs = %s/%s", got.GRPCAddr, got.HTTPAddr)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != want[0].ID || !got.Nodes[0].Leader {
		t.Fatalf("nodes = %+v, want default single leader node", got.Nodes)
	}
	if got.NodeID != mockserver.DefaultNodeID() {
		t.Fatalf("NodeID = %q, want host-derived %q", got.NodeID, mockserver.DefaultNodeID())
	}
}

func TestClusterCommandExplicitFlags(t *testing.T) {
	var got mockserver.Config
	cmd := NewClusterCommand(ClusterDeps{Runner: func(_ context.Context, cfg mockserver.Config) error {
		got = cfg
		return nil
	}})
	cmd.SetArgs([]string{
		"--grpc-addr", "127.0.0.1:9290",
		"--http-addr", "127.0.0.1:9291",
		"--self", "node-1",
		"--node", "id=node-1,address=127.0.0.1:9090,roles=primary,leader=true",
		"--node", "id=node-2,address=127.0.0.1:9092,roles=standby",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.GRPCAddr != "127.0.0.1:9290" || got.HTTPAddr != "127.0.0.1:9291" || got.NodeID != "node-1" {
		t.Fatalf("cfg = %+v", got)
	}
	if len(got.Nodes) != 2 || got.Nodes[1].ID != "node-2" {
		t.Fatalf("nodes = %+v", got.Nodes)
	}
}

// TestClusterCommandConfigFilePrecedence 覆盖 默认值 → 配置文件 → flag 的
// 优先级：文件提供基础值，显式 flag 覆盖 self。
func TestClusterCommandConfigFilePrecedence(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "cluster.yaml")
	yaml := `
cluster:
  mock_server:
    grpc_addr: 127.0.0.1:9190
    http_addr: 127.0.0.1:9191
    self: node-2
    nodes:
      - "id=node-1,address=10.0.0.1:9090,roles=primary,leader=true"
      - "id=node-2,address=10.0.0.2:9090,roles=standby"
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	var got mockserver.Config
	cmd := NewClusterCommand(ClusterDeps{Runner: func(_ context.Context, cfg mockserver.Config) error {
		got = cfg
		return nil
	}})
	cmd.SetArgs([]string{"--config", file, "--self", "node-1"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.GRPCAddr != "127.0.0.1:9190" || got.HTTPAddr != "127.0.0.1:9191" {
		t.Fatalf("addrs from file = %s/%s", got.GRPCAddr, got.HTTPAddr)
	}
	if got.NodeID != "node-1" {
		t.Fatalf("NodeID = %q, want flag override node-1", got.NodeID)
	}
	if len(got.Nodes) != 2 || got.Nodes[0].Address != "10.0.0.1:9090" {
		t.Fatalf("nodes from file = %+v", got.Nodes)
	}
}

func TestClusterCommandBadSpecFails(t *testing.T) {
	called := false
	cmd := NewClusterCommand(ClusterDeps{Runner: func(context.Context, mockserver.Config) error {
		called = true
		return nil
	}})
	cmd.SetArgs([]string{"--node", "leader=true"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute accepted spec without id")
	}
	if called {
		t.Fatal("runner must not be invoked on invalid spec")
	}
}

func TestClusterCommandRunnerErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	cmd := NewClusterCommand(ClusterDeps{Runner: func(context.Context, mockserver.Config) error { return sentinel }})
	if err := cmd.Execute(); !errors.Is(err, sentinel) {
		t.Fatalf("Execute error = %v, want sentinel", err)
	}
}
