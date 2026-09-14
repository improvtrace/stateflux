package rpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dialer 是 gRPC 连接池（§3.2、§6.3）：按目标地址复用连接，供 unary 与 stream
// adapter 共享。连接懒建立；调用失败由通道暴露给上层，但不允许被推断为任务未执行。
type Dialer struct {
	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	opts   []grpc.DialOption
	closed bool
}

// NewDialer 构造连接池；默认使用明文凭据（集群内网），调用方可用 WithDialOptions 覆盖。
func NewDialer(opts ...grpc.DialOption) *Dialer {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &Dialer{conns: map[string]*grpc.ClientConn{}, opts: opts}
}

// Conn 返回目标地址的连接（不存在则建立）。
func (d *Dialer) Conn(address string) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, ErrNoTarget
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errors.New("eventbus/channel/rpc: dialer closed")
	}
	if c, ok := d.conns[address]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(address, d.opts...)
	if err != nil {
		return nil, err
	}
	d.conns[address] = c
	return c, nil
}

// Close 关闭全部连接。
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var firstErr error
	for addr, c := range d.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(d.conns, addr)
	}
	return firstErr
}

// withTimeout 在 timeout > 0 时给 ctx 加超时。
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
