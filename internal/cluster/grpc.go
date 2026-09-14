package cluster

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
)

// grpcView 经 gRPC 调用外部 ClusterService（§15.1#3）。
type grpcView struct {
	conn    *grpc.ClientConn
	client  clusterv1.ClusterServiceClient
	timeout time.Duration
}

// NewGRPCView 构造 gRPC 集群视图客户端。连接懒建立，Unavailable 由每次 Get 暴露。
func NewGRPCView(cfg config.Cluster) (View, error) {
	if cfg.Endpoint == "" {
		return nil, errNoEndpoint("grpc")
	}
	conn, err := grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &grpcView{
		conn:    conn,
		client:  clusterv1.NewClusterServiceClient(conn),
		timeout: cfg.Timeout,
	}, nil
}

// Close 释放连接。
func (v *grpcView) Close() error { return v.conn.Close() }

// Get 拉取一次集群信息快照。
func (v *grpcView) Get(ctx context.Context) (Info, error) {
	if v.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, v.timeout)
		defer cancel()
	}
	resp, err := v.client.GetClusterInfo(ctx, &clusterv1.ClusterInfoRequest{})
	if err != nil {
		return Info{}, err
	}
	return infoFromProto(resp), nil
}
