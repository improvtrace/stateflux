package server

import (
	"context"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/obs"
)

// DefaultElectionPollInterval 是选举归属检查周期。
const DefaultElectionPollInterval = 2 * time.Second

// electionGate 按集群视图的调度节点归属启停控制面组件（§2.1）：Scheduler / Collector /
// Reconciler / Factory / Coherence 同步只在「外部选举指向本实例」时运行。故障切换后由
// 归属变化自然接管；短暂双主窗口由 R1 与 attempt 幂等收敛（§6.2，不做 PG fencing）。
type electionGate struct {
	name       string
	cache      cluster.ClusterCacheView
	self       string
	permission cluster.NodePermission
	inner      Component
	poll       time.Duration
	metrics    *obs.Metrics

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewElectionGate 包装一个控制面组件；cache 为 nil 时退化为「始终运行」（静态单机）。
// permission 为该组件要求的服务权限（cluster.NodePermission，空 = 不限制）：本节点必须
// 当选 leader 且快照中自身节点启用该权限，inner 才会运行。
func NewElectionGate(name string, cache cluster.ClusterCacheView, self string, permission cluster.NodePermission, inner Component, poll time.Duration, metrics *obs.Metrics) Component {
	if poll <= 0 {
		poll = DefaultElectionPollInterval
	}
	return &electionGate{name: name, cache: cache, self: self, permission: permission, inner: inner, poll: poll, metrics: metrics}
}

// Start 启动归属观察循环（不直接启动 inner）。
func (g *electionGate) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	g.mu.Lock()
	g.cancel = cancel
	g.done = make(chan struct{})
	done := g.done
	g.mu.Unlock()

	// 立即同步一次归属，缩短启动窗口。
	g.sync(runCtx)
	go func() {
		defer close(done)
		t := time.NewTicker(g.poll)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				g.stopInner()
				return
			case <-t.C:
				g.sync(runCtx)
			}
		}
	}()
	return nil
}

// Stop 停止观察并停掉 inner（幂等）。
func (g *electionGate) Stop() error {
	g.mu.Lock()
	cancel := g.cancel
	done := g.done
	g.cancel = nil
	g.done = nil
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

func (g *electionGate) sync(ctx context.Context) {
	want := g.isScheduler()
	g.mu.Lock()
	running := g.running
	g.mu.Unlock()
	switch {
	case want && !running:
		if err := g.inner.Start(ctx); err == nil {
			g.mu.Lock()
			g.running = true
			g.mu.Unlock()
		}
	case !want && running:
		g.stopInner()
	}
}

func (g *electionGate) stopInner() {
	g.mu.Lock()
	running := g.running
	g.running = false
	g.mu.Unlock()
	if running {
		_ = g.inner.Stop()
	}
}

// isScheduler 判断当前实例是否为选举出的调度节点且具备所需服务权限；
// 无集群视图时始终为真（单机闭环）。视图报告「暂无调度节点」时保守判定为
// 非调度节点：等待选举结果，不由每个实例自行接管。
func (g *electionGate) isScheduler() bool {
	if g.cache == nil {
		return true
	}
	snapshot := g.cache.Snapshot()
	if snapshot.SchedulerNodeID == "" || snapshot.SchedulerNodeID != g.self {
		return false
	}
	if g.permission == "" {
		return true
	}
	// 快照未包含自身节点、或未下发服务权限时保守放行：
	// 权限信息缺失不应阻断选举语义。
	node, ok := snapshot.Node(g.self)
	if !ok || len(node.Permissions) == 0 {
		return true
	}
	return node.HasPermission(g.permission)
}
