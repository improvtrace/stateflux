package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/improvtrace/stateflux/proto/gen/clusterv1"
)

type clusterSnapshot struct {
	nodes       []Node
	schedulerID string
}

type grpcClientConn struct {
	cc *grpc.ClientConn
	sync.Once
	closeErr error
}

func (c *RPCClient) conn2() (*grpc.ClientConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		opts := []grpc.DialOption{}
		if c.insecure {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		cc, err := grpc.NewClient(c.endpoint, opts...)
		if err != nil {
			return nil, fmt.Errorf("cluster: dial %s: %w", c.endpoint, err)
		}
		c.conn = &grpcClientConn{cc: cc}
	}
	return c.conn.cc, nil
}

// Close 释放底层连接。
func (c *RPCClient) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		c.conn.Do(func() { c.conn.closeErr = c.conn.cc.Close() })
	}
	return nil
}

// fetch 执行一次 GetClusterInfo RPC。
func (c *RPCClient) fetch(ctx context.Context) (clusterSnapshot, error) {
	cc, err := c.conn2()
	if err != nil {
		return clusterSnapshot{}, err
	}
	client := clusterv1.NewClusterServiceClient(cc)
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := client.GetClusterInfo(callCtx, &clusterv1.ClusterInfoRequest{})
	if err != nil {
		return clusterSnapshot{}, err
	}
	snap := clusterSnapshot{schedulerID: resp.GetSchedulerNodeId()}
	for _, n := range resp.GetNodes() {
		snap.nodes = append(snap.nodes, Node{
			ID:           n.GetNodeId(),
			Address:      n.GetAddress(),
			Roles:        n.GetRoles(),
			Capabilities: n.GetCapabilities(),
		})
	}
	return snap, nil
}
