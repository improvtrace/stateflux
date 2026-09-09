package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
)

// Pool 执行节点 gRPC 客户端池（懒建连接，复用长连接）。
// 调度侧的同步分发（Execute）与归集侧的拉取/回执（Collect/Ack）共用。
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPool 构造连接池。
func NewPool() *Pool { return &Pool{conns: make(map[string]*grpc.ClientConn)} }

func (p *Pool) client(addr string) (dispatchv1.ExecutorServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cc, ok := p.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("executor pool: dial %s: %w", addr, err)
		}
		p.conns[addr] = cc
	}
	return dispatchv1.NewExecutorServiceClient(cc), nil
}

// callCtx 单次 RPC 超时。
func callCtx(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 30 * time.Second
	}
	return context.WithTimeout(ctx, d)
}

// Execute 同步任务调用（结果需及时回执，§5.3）。
func (p *Pool) Execute(ctx context.Context, addr string, req *dispatchv1.ExecuteRequest, timeout time.Duration) (*dispatchv1.ExecuteResponse, error) {
	client, err := p.client(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := callCtx(ctx, timeout)
	defer cancel()
	return client.Execute(cctx, req)
}

// Collect 拉取未 Ack 结果（§5.5 pull 模型）。
func (p *Pool) Collect(ctx context.Context, addr string, limit int) (*dispatchv1.CollectResponse, error) {
	client, err := p.client(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := callCtx(ctx, 10*time.Second)
	defer cancel()
	return client.Collect(cctx, &dispatchv1.CollectRequest{Limit: int32(limit)})
}

// Ack 归集回执（§5.5）。
func (p *Pool) Ack(ctx context.Context, addr string, taskIDs []int64) error {
	client, err := p.client(addr)
	if err != nil {
		return err
	}
	cctx, cancel := callCtx(ctx, 10*time.Second)
	defer cancel()
	_, err = client.Ack(cctx, &dispatchv1.AckRequest{TaskIds: taskIDs})
	return err
}

// Close 关闭全部连接。
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, cc := range p.conns {
		_ = cc.Close()
		delete(p.conns, addr)
	}
	return nil
}
