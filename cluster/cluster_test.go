package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/improvtrace/stateflux/proto/gen/clusterv1"
)

// fakeClusterService 外部选举服务的桩（§7：stateflux 只消费 GetClusterInfo）。
type fakeClusterService struct {
	clusterv1.UnimplementedClusterServiceServer
	resp *clusterv1.ClusterInfoResponse
}

func (f *fakeClusterService) GetClusterInfo(context.Context, *clusterv1.ClusterInfoRequest) (*clusterv1.ClusterInfoResponse, error) {
	return f.resp, nil
}

// TestRPCClientGetClusterInfo 外部契约消费：节点列表/角色/能力标签/调度节点位置。
func TestRPCClientGetClusterInfo(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	clusterv1.RegisterClusterServiceServer(srv, &fakeClusterService{resp: &clusterv1.ClusterInfoResponse{
		Nodes: []*clusterv1.NodeInfo{
			{NodeId: "n1", Address: "10.0.0.1:7001", Roles: []string{"api", "executor"}, Capabilities: []string{"gpu"}},
			{NodeId: "n2", Address: "10.0.0.2:7001", Roles: []string{"api", "executor", "scheduler"}},
		},
		SchedulerNodeId: "n2",
	}})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	client := NewRPCClient(lis.Addr().String(), "n2", "10.0.0.2:7001", nil, 100*time.Millisecond, true)
	if err := client.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes := client.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("nodes: %d", len(nodes))
	}
	if client.SchedulerID() != "n2" {
		t.Fatalf("scheduler id: %s", client.SchedulerID())
	}
	executors := client.ExecutorNodes()
	if len(executors) != 2 {
		t.Fatalf("executor nodes: %d", len(executors))
	}
	if !executors[0].HasCapability("gpu") || executors[0].HasCapability("tpu") {
		t.Fatal("capability matching broken")
	}
	self := client.Self()
	if self.ID != "n2" {
		t.Fatalf("self: %+v", self)
	}
}

// TestStaticView 单机/开发模式：全部角色赋给本进程（§2.1）。
func TestStaticView(t *testing.T) {
	v := NewStatic(StaticConfig{NodeID: "solo", Address: "127.0.0.1:7001", Capabilities: []string{"ssd"}})
	if v.SchedulerID() != "solo" {
		t.Fatalf("scheduler: %s", v.SchedulerID())
	}
	if len(v.ExecutorNodes()) != 1 || !v.Self().HasCapability("ssd") {
		t.Fatal("static view broken")
	}
}
