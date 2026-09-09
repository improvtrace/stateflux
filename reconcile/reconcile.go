// Package reconcile 实现对账角色（§6.3，运行于调度节点，周期 30s）：
// 以 PG 为准绳、inprocess 为实时视图，双向核对。
//
//	R1 重置：processing 滞留超 grace 且不在 inprocess 或租约已过期 → 挪回 schedulable
//	        （attempt+1 由下次认领承担；run_at = now + 退避；attempts 耗尽 → R3 dead）；
//	R2 幽灵清理：inprocess 中存在但 PG 已终态/不存在的条目 → 强制移除；
//	R3 死信：重置时 attempts >= max_attempts → completed{dead}（store.Requeue 内路由）；
//	R4 重建：Redis 丢失后，扫全部 processing 按 R1 重置并重建集合结构。
package reconcile

import (
	"context"
	"log/slog"
	"time"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// Reconciler 对账角色。
type Reconciler struct {
	store   store.Store
	q       *queue.Queue
	cfg     config.ReconcileConfig
	metrics *obs.Metrics
	log     *slog.Logger
}

// Options 对账角色构造参数。
type Options struct {
	Store   store.Store
	Queue   *queue.Queue
	Cfg     config.ReconcileConfig
	Metrics *obs.Metrics
	Logger  *slog.Logger
}

// New 构造对账角色。
func New(opts Options) *Reconciler {
	opts.Cfg.ApplyDefaults()
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics, _ = obs.New()
	}
	return &Reconciler{
		store:   opts.Store,
		q:       opts.Queue,
		cfg:     opts.Cfg,
		metrics: metrics,
		log:     log,
	}
}

// Run 阻塞运行对账循环，直到 ctx 取消。rebuildOnStart 时启动即执行一次 R4。
func (r *Reconciler) Run(ctx context.Context) {
	if r.cfg.RebuildOnStart {
		if err := r.RebuildAll(ctx); err != nil {
			r.log.ErrorContext(ctx, "reconcile: rebuild on start failed", "err", err)
		}
	}
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := r.Pass(ctx); err != nil {
			r.log.WarnContext(ctx, "reconcile: pass failed", "err", err)
		}
	}
}

// Pass 单轮对账：R1 重置 + R3 死信路由 + R2 幽灵清理。
func (r *Reconciler) Pass(ctx context.Context) error {
	if err := r.reconcileStale(ctx); err != nil {
		return err
	}
	return r.cleanGhosts(ctx)
}

// reconcileStale R1：processing 中 updated_at 超过 grace，且不在 inprocess 或租约已过期 → 重置。
func (r *Reconciler) reconcileStale(ctx context.Context) error {
	now := time.Now()
	cutoff := now.Add(-r.cfg.Grace)
	for {
		tasks, err := r.store.ScanProcessing(ctx, cutoff, r.cfg.Batch)
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			return nil
		}
		alive := r.aliveInprocess(ctx, now)
		entries := make([]store.RequeueEntry, 0, len(tasks))
		var (
			byLeaseExpired int64
			byMissing      int64
		)
		for _, t := range tasks {
			leaseAt, inSet := alive[t.ID]
			if inSet && leaseAt.After(now) {
				continue // 仍在租约内（执行中/结果未归集）——不重置
			}
			reason := "stale-lease-expired"
			if !inSet {
				reason = "stale-not-in-inprocess"
				byMissing++
			} else {
				byLeaseExpired++
			}
			entries = append(entries, store.RequeueEntry{
				TaskID:  t.ID,
				Attempt: t.Attempts,
				RunAt:   now.Add(sdk.Backoff(t.Attempts)), // capped exponential + full jitter（§6.3）
				Error:   "reconcile reset: " + reason,
			})
		}
		if len(entries) == 0 {
			return nil
		}
		report, err := r.store.Requeue(ctx, entries)
		if err != nil {
			return err
		}
		r.metrics.ResetCount(ctx, "r1-requeue", int64(report.Requeued))
		r.metrics.ResetCount(ctx, "r3-dead", int64(report.Dead))
		r.log.WarnContext(ctx, "reconcile: reset stale tasks",
			"requeued", report.Requeued, "dead", report.Dead, "skipped", report.Skipped,
			"lease_expired", byLeaseExpired, "not_in_inprocess", byMissing)
		// 本批全部为滞留任务；若恰好等于 batch，可能还有更多，继续扫。
		if len(tasks) < r.cfg.Batch {
			return nil
		}
	}
}

// aliveInprocess inprocess 实时视图：task_id → 租约到期时间。
func (r *Reconciler) aliveInprocess(ctx context.Context, now time.Time) map[int64]time.Time {
	entries, err := r.q.ListInprocess(ctx)
	if err != nil {
		r.log.WarnContext(ctx, "reconcile: list inprocess failed", "err", err)
		return nil
	}
	out := make(map[int64]time.Time, len(entries))
	for _, e := range entries {
		out[e.TaskID] = e.LeaseAt
	}
	return out
}

// cleanGhosts R2：inprocess 中存在但 PG 已终态/不存在的条目 → 强制移除。
func (r *Reconciler) cleanGhosts(ctx context.Context) error {
	entries, err := r.q.ListInprocess(ctx)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.TaskID)
	}
	inProcessing, err := r.store.ExistsProcessing(ctx, ids)
	if err != nil {
		return err
	}
	var ghosts []int64
	for _, id := range ids {
		if !inProcessing[id] {
			ghosts = append(ghosts, id)
		}
	}
	if len(ghosts) == 0 {
		return nil
	}
	if err := r.q.ForceRemove(ctx, ghosts); err != nil {
		return err
	}
	r.log.InfoContext(ctx, "reconcile: removed ghost inprocess entries", "count", len(ghosts))
	return nil
}

// RebuildAll R4（§6.3）：Redis 全量丢失后的重建——扫全部 processing 按 R1 重置。
// 任务在 Redis 中已无成员/队列痕迹，重置后由调度重新认领投递（at-least-once，业务幂等收敛）。
func (r *Reconciler) RebuildAll(ctx context.Context) error {
	r.log.WarnContext(ctx, "reconcile: R4 rebuild all processing tasks")
	cutoff := time.Now().Add(24 * time.Hour) // 全量：updated_at 不可能晚于该值
	total := 0
	for {
		tasks, err := r.store.ScanProcessing(ctx, cutoff, r.cfg.Batch)
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			break
		}
		entries := make([]store.RequeueEntry, 0, len(tasks))
		for _, t := range tasks {
			entries = append(entries, store.RequeueEntry{
				TaskID:  t.ID,
				Attempt: t.Attempts,
				RunAt:   time.Now().Add(sdk.Backoff(t.Attempts)),
				Error:   "reconcile rebuild (R4)",
			})
		}
		report, err := r.store.Requeue(ctx, entries)
		if err != nil {
			return err
		}
		total += report.Requeued + report.Dead
		if len(tasks) < r.cfg.Batch {
			break
		}
	}
	r.metrics.ResetCount(ctx, "r4-rebuild", int64(total))
	r.log.WarnContext(ctx, "reconcile: R4 rebuild done", "reset", total)
	return nil
}
