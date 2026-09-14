// Package reconcile 实现 R1–R4 对账（§6.2）：它是全流程收敛性的兜底——通道丢失、
// Redis 灾难都只导致 processing 在 grace 后重置重投，而不依赖任何通道的可靠性或
// Redis 状态。仅调度节点运行。
//
// R1 processing 超时重置（本包主路径）；R2 清理可选 Redis 视图；R3 死信由 Collector
// 在重试路径内完成；R4「所有 channel 不可用后的扫描重投」并入 R1（§14.9 的待定项）。
package reconcile

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/internal/obs"
)

// Ledger 是对账所需的最小账本面（由 repository.Store 实现，§6.2）。
type Ledger interface {
	// ResetExpired 把 updated_at 早于 deadline 的 processing 行移回 schedulable（R1）。
	ResetExpired(ctx context.Context, deadline time.Time, limit int) (int, error)
}

// View 是可选 Redis 视图的清理面（R2，§6.2）。
type View interface {
	Purge(ctx context.Context, prefix string) (int64, error)
}

// Options 是 Reconciler 装配参数。
type Options struct {
	Ledger Ledger
	View   View
	// Interval 是 R1 扫描周期。
	Interval time.Duration
	// Grace 是 dispatch grace（§10 默认 90s）。
	Grace time.Duration
	// Skew 是时钟/调度偏差余量（§14.11 待定，默认 10s）。
	Skew time.Duration
	// Limit 是单轮 R1 重置上限。
	Limit int
	// PurgePrefix 非空时启用 R2 清理；PurgeEvery 控制清理频率（以轮计）。
	PurgePrefix string
	PurgeEvery  int
	Metrics     *obs.Metrics
}

// Reconciler 周期性执行 R1（并可选 R2）。
type Reconciler struct {
	ledger   Ledger
	view     View
	interval time.Duration
	grace    time.Duration
	skew     time.Duration
	limit    int
	prefix   string
	every    int
	metrics  *obs.Metrics

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
	rounds int
}

// New 构造 Reconciler。
func New(opts Options) *Reconciler {
	interval := opts.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	grace := opts.Grace
	if grace <= 0 {
		grace = 90 * time.Second
	}
	skew := opts.Skew
	if skew < 0 {
		skew = 0
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 500
	}
	every := opts.PurgeEvery
	if every <= 0 {
		every = 30
	}
	return &Reconciler{
		ledger:   opts.Ledger,
		view:     opts.View,
		interval: interval,
		grace:    grace,
		skew:     skew,
		limit:    limit,
		prefix:   opts.PurgePrefix,
		every:    every,
		metrics:  opts.Metrics,
	}
}

// Start 启动对账循环（非阻塞）；重复 Start 返回错误。
func (r *Reconciler) Start(ctx context.Context) error {
	if r.ledger == nil {
		return errors.New("runtime/reconcile: nil ledger")
	}
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return errors.New("runtime/reconcile: already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTicker(r.interval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				_, _ = r.RunOnce(runCtx)
			}
		}
	}()
	return nil
}

// Stop 停止对账循环（幂等）。
func (r *Reconciler) Stop() error {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	return nil
}

// RunOnce 执行一轮对账：R1 重置 +（可选）R2 清理。
func (r *Reconciler) RunOnce(ctx context.Context) (int, error) {
	deadline := time.Now().Add(-(r.grace + r.skew))
	reset, err := r.ledger.ResetExpired(ctx, deadline, r.limit)
	if err != nil {
		if r.metrics != nil {
			r.metrics.ResetCount(ctx, "r1_error", 1)
		}
		return 0, err
	}
	if reset > 0 && r.metrics != nil {
		r.metrics.ResetCount(ctx, "r1_timeout", int64(reset))
	}
	r.mu.Lock()
	r.rounds++
	doPurge := r.prefix != "" && r.view != nil && r.rounds%r.every == 0
	r.mu.Unlock()
	if doPurge {
		// R2：只清理视图，不据此修改 PG（§6.2）。
		_, _ = r.view.Purge(ctx, r.prefix)
	}
	return reset, nil
}
