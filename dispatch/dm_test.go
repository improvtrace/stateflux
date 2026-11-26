package dispatch

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	dmpb "github.com/improvtrace/stateflux/api/dm"
)

const localDMKey = "local-dm-key"

// localDM 是 dm 桩：返回固定的 node_id，服务密钥校验与真实 dm 一致。
type localDM struct {
	dmpb.UnimplementedServiceServiceServer
	nodeID string
}

func (s localDM) ListClusterNodesStatus(ctx context.Context, _ *emptypb.Empty) (*dmpb.ListClusterNodesStatusResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get(fieldServiceKey)) == 0 || md.Get(fieldServiceKey)[0] != localDMKey {
		return nil, status.Error(codes.Unauthenticated, "service_key validation failed")
	}
	return &dmpb.ListClusterNodesStatusResponse{NodeId: s.nodeID}, nil
}

// startLocalDM 在临时目录的 unix socket 上启动 dm 桩，返回 socket 路径。
func startLocalDM(t *testing.T, nodeID string) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "dm.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("监听 dm socket 失败: %v", err)
	}
	srv := grpc.NewServer()
	dmpb.RegisterServiceServiceServer(srv, localDM{nodeID: nodeID})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return sock
}

// TestLocalNodeID 断言本节点 ID 取自 ListClusterNodesStatus 响应的 node_id：
// 空值即报错（没有节点标识就无法确定分节点队列），服务密钥经拦截器携带。
func TestLocalNodeID(t *testing.T) {
	ctx := context.Background()

	id, err := LocalNodeID(ctx, startLocalDM(t, "node-2"), localDMKey)
	assert.NoError(t, err)
	assert.Equal(t, "node-2", id)

	_, err = LocalNodeID(ctx, startLocalDM(t, ""), localDMKey)
	assert.ErrorContains(t, err, "empty node_id")

	_, err = LocalNodeID(ctx, startLocalDM(t, "node-1"), "wrong-key")
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = LocalNodeID(ctx, filepath.Join(t.TempDir(), "absent.sock"), localDMKey)
	assert.Error(t, err)
}
