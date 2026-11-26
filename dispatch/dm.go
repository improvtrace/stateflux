package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	dmpb "github.com/improvtrace/stateflux/api/dm"
)

// fieldServiceKey 是 dm 服务内部接口的鉴权 metadata 键。
const fieldServiceKey = "service_key"

// dmCluster 查询集群节点的 dm 服务客户端：unix socket 懒连接 + 服务密钥
// 拦截器（dm 服务监听地址即 socket 路径）。proto 定义见 api/dm。
type dmCluster struct {
	client     dmpb.ServiceServiceClient
	underConn  *grpc.ClientConn
	address    string
	serviceKey string

	started bool
	lock    *sync.Mutex
}

// newDMCluster 构造 dm 客户端，连接延迟到首次查询。
func newDMCluster(address, serviceKey string) *dmCluster {
	return &dmCluster{address: address, serviceKey: serviceKey, lock: &sync.Mutex{}}
}

// connect 懒连接 dm 服务。
func (c *dmCluster) connect() error {
	if c.started {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.client != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), grpc.WithContextDialer(unixConnect),
		grpc.WithUnaryInterceptor(serviceKeyInterceptor(c.serviceKey)),
	}
	slog.Debug("dispatch: connecting to dm service", "address", c.address)
	conn, err := grpc.DialContext(ctx, c.address, opts...)
	if err != nil {
		slog.Error("dispatch: failed to connect to dm service", "address", c.address, "error", err)
		return err
	}
	c.underConn = conn
	c.client = dmpb.NewServiceServiceClient(conn)
	c.started = true
	slog.Info("dispatch: connected to dm service", "address", c.address)
	return nil
}

// listClusterNodesStatus 查询集群节点状态。
func (c *dmCluster) listClusterNodesStatus(ctx context.Context) (*dmpb.ListClusterNodesStatusResponse, error) {
	if err := c.connect(); err != nil {
		return nil, err
	}
	start := time.Now()
	resp, err := c.client.ListClusterNodesStatus(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	slog.Debug("dispatch: dm request completed",
		"method", "list_cluster_nodes_status", "nodes", len(resp.GetNodes()),
		"duration_ms", time.Since(start).Milliseconds())
	return resp, nil
}

// close 关闭底层连接；连接尚未建立时为空操作。
func (c *dmCluster) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.underConn != nil {
		_ = c.underConn.Close()
	}
	c.underConn = nil
	c.client = nil
	c.started = false
}

// nodeIDTimeout 是启动时查询本节点 ID 的时限。
const nodeIDTimeout = 5 * time.Second

// LocalNodeID 经 dm 服务查询本节点 ID：ListClusterNodesStatus 响应携带的
// node_id。dm 服务随节点部署、经本地 unix socket 应答，该字段即应答节点
// 自身——调度器以它作为 claimed_node、worker consumerTag 与分节点队列名，
// 不再由配置指定。
func LocalNodeID(ctx context.Context, address, serviceKey string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, nodeIDTimeout)
	defer cancel()
	c := newDMCluster(address, serviceKey)
	defer c.close()
	resp, err := c.listClusterNodesStatus(ctx)
	if err != nil {
		return "", fmt.Errorf("dispatch: query local node id via %s: %w", address, err)
	}
	id := resp.GetNodeId()
	if id == "" {
		return "", errors.New("dispatch: dm returned empty node_id")
	}
	slog.Debug("dispatch: local node id resolved", "node", id)
	return id, nil
}

// serviceKeyInterceptor 为出站请求附加服务密钥（metadata: service_key）。
func serviceKeyInterceptor(key string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, fieldServiceKey, key)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// unixConnect 拨号 unix socket。
func unixConnect(ctx context.Context, address string) (net.Conn, error) {
	unixAddress, err := net.ResolveUnixAddr("unix", address)
	if err != nil {
		return nil, err
	}
	return net.DialUnix("unix", nil, unixAddress)
}
