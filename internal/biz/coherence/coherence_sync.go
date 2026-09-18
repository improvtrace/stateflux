package coherence

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	coherencev1 "github.com/improvtrace/stateflux/api/stateflux/coherence/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
	"github.com/improvtrace/stateflux/internal/obs"
)

// CoherenceSyncer 是调度侧的共识同步运行时（§15.1#4、§15.3#3）：周期计算
// 「异步队列 → 执行节点」映射，应用本地快照并 Notify 给执行节点。
type CoherenceSyncer struct {
	store     *CoherenceStore
	allocator Allocator
	pusher    *CoherencePusher
	nodes     cluster.ClusterCacheView
	queues    []string
	interval  time.Duration
	metrics   *obs.Metrics

	revision atomic.Int64
	mu       sync.Mutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// CoherenceSyncerOptions 是装配参数。
type CoherenceSyncerOptions struct {
	Store     *CoherenceStore
	Allocator Allocator
	Pusher    *CoherencePusher
	Nodes     cluster.ClusterCacheView
	Queues    []string
	Interval  time.Duration
	Metrics   *obs.Metrics
}

// NewCoherenceSyncer 构造调度侧同步器。
func NewCoherenceSyncer(opts CoherenceSyncerOptions) *CoherenceSyncer {
	interval := opts.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	s := &CoherenceSyncer{
		store:     opts.Store,
		allocator: opts.Allocator,
		pusher:    opts.Pusher,
		nodes:     opts.Nodes,
		queues:    append([]string(nil), opts.Queues...),
		interval:  interval,
		metrics:   opts.Metrics,
	}
	s.revision.Store(opts.Store.Revision())
	return s
}

// Start 启动周期同步（非阻塞）；重复 Start 返回错误。
func (s *CoherenceSyncer) Start(ctx context.Context) error {
	if s.store == nil {
		return errors.New("biz: coherence syncer requires a store")
	}
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return errors.New("biz: coherence syncer already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.mu.Unlock()

	// 立即同步一次，缩短启动空窗。
	_ = s.SyncOnce(runCtx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				_ = s.SyncOnce(runCtx)
			}
		}
	}()
	return nil
}

// Stop 停止同步（幂等）。
func (s *CoherenceSyncer) Stop() error {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	return nil
}

// SyncOnce 计算并推送一份新快照。
func (s *CoherenceSyncer) SyncOnce(ctx context.Context) error {
	snapshot := cluster.Info{}
	if s.nodes != nil {
		snapshot = s.nodes.Snapshot()
	}
	revision := s.revision.Add(1)
	state := s.allocator.Assign(s.queues, snapshot.Nodes, revision, time.Now().UnixMilli())
	if !s.store.Apply(state) {
		return nil
	}
	if s.metrics != nil {
		s.metrics.CoherenceRevisionRecord(ctx, revision)
	}
	if s.pusher == nil {
		return nil
	}
	ids := make([]string, 0, len(snapshot.Nodes))
	for _, n := range snapshot.Nodes {
		if n.ID == "" {
			continue
		}
		ids = append(ids, n.ID)
	}
	if err := s.pusher.PushAll(ctx, state, ids); err != nil && s.metrics != nil {
		s.metrics.CoherenceSyncRecord(ctx, "error", 1)
		return err
	}
	if s.metrics != nil {
		s.metrics.CoherenceSyncRecord(ctx, "ok", 1)
	}
	return nil
}

// CoherencePuller 是执行侧的共识拉取运行时：启动/周期从调度节点拉取快照并应用，
// 作为 Notify 推送丢失时的兜底（§15.1#4）。
type CoherencePuller struct {
	store    *CoherenceStore
	view     cacheviewSetter
	dialer   *rpc.Dialer
	nodes    cluster.Resolver
	self     string
	interval time.Duration
	timeout  time.Duration
	metrics  *obs.Metrics

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type cacheviewSetter interface {
	SetQueueRoute(ctx context.Context, queue, nodeID string) error
}

// CoherencePullerOptions 是装配参数。
type CoherencePullerOptions struct {
	Store    *CoherenceStore
	View     cacheviewSetter
	Dialer   *rpc.Dialer
	Nodes    cluster.Resolver
	Self     string
	Interval time.Duration
	Timeout  time.Duration
	Metrics  *obs.Metrics
}

// NewCoherencePuller 构造执行侧拉取器。
func NewCoherencePuller(opts CoherencePullerOptions) *CoherencePuller {
	interval := opts.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &CoherencePuller{
		store:    opts.Store,
		view:     opts.View,
		dialer:   opts.Dialer,
		nodes:    opts.Nodes,
		self:     opts.Self,
		interval: interval,
		timeout:  timeout,
		metrics:  opts.Metrics,
	}
}

// Start 启动周期拉取。
func (p *CoherencePuller) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.cancel != nil {
		p.mu.Unlock()
		return errors.New("biz: coherence puller already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.mu.Unlock()
	_ = p.PullOnce(runCtx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(p.interval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				_ = p.PullOnce(runCtx)
			}
		}
	}()
	return nil
}

// Stop 停止拉取（幂等）。
func (p *CoherencePuller) Stop() error {
	p.mu.Lock()
	cancel := p.cancel
	p.cancel = nil
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wg.Wait()
	return nil
}

// PullOnce 从当前调度节点拉取一次快照并应用。
func (p *CoherencePuller) PullOnce(ctx context.Context) error {
	if p.dialer == nil || p.nodes == nil {
		return errors.New("biz: coherence puller missing dialer or cluster view")
	}
	scheduler, ok := p.nodes.Node(SchedulerNodeID(p.nodes))
	if !ok || scheduler.Address == "" {
		return errors.New("biz: no scheduler node to pull coherence from")
	}
	conn, err := p.dialer.Conn(scheduler.Address)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := coherencev1.NewCoherenceServiceClient(conn).GetCoherence(callCtx, &coherencev1.GetCoherenceRequest{
		NodeId:        p.self,
		SinceRevision: p.store.Revision(),
	})
	if err != nil {
		if p.metrics != nil {
			p.metrics.CoherenceSyncRecord(ctx, "error", 1)
		}
		return err
	}
	if resp.GetNotModified() {
		if p.metrics != nil {
			p.metrics.CoherenceSyncRecord(ctx, "not_modified", 1)
		}
		return nil
	}
	if !p.store.Apply(resp.GetState()) {
		return nil
	}
	if p.view != nil {
		for _, r := range resp.GetState().GetRoutes() {
			_ = p.view.SetQueueRoute(ctx, r.GetQueue(), r.GetNodeId())
		}
	}
	if p.metrics != nil {
		p.metrics.CoherenceSyncRecord(ctx, "ok", 1)
	}
	return nil
}

func SchedulerNodeID(nodes cluster.Resolver) string {
	type snapshotter interface{ Snapshot() cluster.Info }
	if s, ok := nodes.(snapshotter); ok {
		return s.Snapshot().SchedulerNodeID
	}
	return ""
}
