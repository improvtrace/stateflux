package cluster

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
)

// grpcView 经 gRPC 调用外部 ClusterService（§15.1#3），对应 dsn：grpc://host:port。
// 连接与调用参数：connect_timeout → 连接建立超时，timeout → 单次 Get 预算。
type grpcView struct {
	conn           *grpc.ClientConn
	client         clusterv1.ClusterServiceClient
	timeout        time.Duration
	connectTimeout time.Duration
}

// NewGRPCView 构造 gRPC 集群视图客户端。连接懒建立，Unavailable 由每次 Get 暴露。
func NewGRPCView(opts config.ClusterOptions) (View, error) {
	if opts.Host == "" {
		return nil, errNoEndpoint("grpc")
	}
	conn, err := grpc.NewClient(opts.Host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			// grpc v1.35+ 语义：MinConnectTimeout 为单次连接建立的时间下限。
			MinConnectTimeout: opts.ConnectTimeout,
			Backoff:           backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, MaxDelay: opts.ConnectTimeout},
		}))
	if err != nil {
		return nil, err
	}
	return &grpcView{
		conn:           conn,
		client:         clusterv1.NewClusterServiceClient(conn),
		timeout:        opts.Timeout,
		connectTimeout: opts.ConnectTimeout,
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
