package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// Promoter 调度预处理——约束晋升器（§5.2）：扫描 pending（run_at 到期）→ 约束评估通过 →
// 批量挪入 schedulable。可调度性在这里判定，认领面只剩小而纯的就绪集。
// 双触发：定时 tick + NOTIFY/容量信号驱动；tick 兜底不可关（§5.1 NOTIFY 实施约束）。
type Promoter struct {
	store    store.Store
	interval time.Duration
	batch    int
	concur   map[string]int // 内置约束：per-type 全局并发上限
	metrics  *obs.Metrics
	log      *slog.Logger

	mu   sync.RWMutex
	hook map[string]sdk.Precondition
}

// PromoterOptions 晋升器构造参数。
type PromoterOptions struct {
	Store           store.Store
	Interval        time.Duration
	Batch           int
	TypeConcurrency map[string]int
	Metrics         *obs.Metrics
	Logger          *slog.Logger
}

// NewPromoter 构造晋升器。
func NewPromoter(opts PromoterOptions) *Promoter {
	if opts.Interval <= 0 {
		opts.Interval = 200 * time.Millisecond
	}
	if opts.Batch <= 0 {
		opts.Batch = 1000
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics, _ = obs.New()
	}
	return &Promoter{
		store:    opts.Store,
		interval: opts.Interval,
		batch:    opts.Batch,
		concur:   opts.TypeConcurrency,
		metrics:  metrics,
		log:      log,
		hook:     make(map[string]sdk.Precondition),
	}
}

// RegisterPrecondition 注册业务约束钩子（§5.2，业务经 sdk 注册；须在 Run 前完成）。
func (p *Promoter) RegisterPrecondition(taskType string, fn sdk.Precondition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hook[taskType] = fn
}

// Run 阻塞运行晋升循环，直到 ctx 取消。
func (p *Promoter) Run(ctx context.Context, wake <-chan struct{}) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake: // NOTIFY/容量信号驱动（低延迟优化；tick 兜底）
		}
		p.tick(ctx)
	}
}

func (p *Promoter) tick(ctx context.Context) {
	p.mu.RLock()
	hooks := p.hook
	p.mu.RUnlock()
	stats, err := p.store.Promote(ctx, store.PromoteOptions{
		Now:             time.Now(),
		Limit:           p.batch,
		TypeConcurrency: p.concur,
		Preconditions:   hooks,
	})
	if err != nil {
		p.log.WarnContext(ctx, "promoter: promote failed", "err", err)
		return
	}
	if stats.Scanned > 0 {
		p.log.DebugContext(ctx, "promoter: tick",
			"scanned", stats.Scanned, "promoted", stats.Promoted,
			"blocked_concurrency", stats.BlockedConcurrency,
			"blocked_precondition", stats.BlockedPrecondition)
	}
}
