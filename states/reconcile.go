package states

import (
	"context"
	"log/slog"
	"time"

	"github.com/improvtrace/stateflux/queue"
)

const (
	defaultReconcileInterval = 30 * time.Second // 对账周期缺省值
	defaultReconcileGrace    = 30 * time.Second // 超时判定的额外缓冲（时钟偏移与发布宽限）
	reconcileBatch           = 100              // 单轮对账的行数上限
	minExecBudget            = 30 * time.Second // 行级超时下限，防 timeout_ms 过小误判
)

// PendingDispatcher 提供 pending 行的重派能力（消费侧小接口，由根包
// publisher 实现）：types 为空 = 全部类型。
type PendingDispatcher interface {
	DispatchPending(ctx context.Context, types []string)
}

// Reconciler 周期对账：把在途超时的行（两阶段任一步卡住：dispatching 未发布
// 确认、dispatched 未终态）收敛——查 queue backend 的任务终态，有终态按终态
// 落表；无记录（Redis 重启/未执行）则回 pending，由恢复通道重新 dispatch。
// 服务重启/停止后未执行完的任务（pending/dispatching/dispatched）由 entry
// 启动时的首轮对账统一重派到任务队列。注意：RETRY 中的任务被重派会与框架
// 重发并存，重复执行由消费端按 IdempotencyKey 幂等兜底（至少一次语义）。
type Reconciler struct {
	repo     TaskPersistentRepo
	backend  queue.Backend
	ledger   *Ledger
	pub      PendingDispatcher
	interval time.Duration
	grace    time.Duration
}

// NewReconciler 构造对账器，interval/grace <=0 时取缺省值。pub 是发布通道，
// 重派 pending 行用。
func NewReconciler(repo TaskPersistentRepo, backend queue.Backend, ledger *Ledger, pub PendingDispatcher, interval, grace time.Duration) *Reconciler {
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	if grace <= 0 {
		grace = defaultReconcileGrace
	}
	return &Reconciler{repo: repo, backend: backend, ledger: ledger, pub: pub, interval: interval, grace: grace}
}

// Run 周期对账直到 ctx 取消，首轮立即执行。
func (r *Reconciler) Run(ctx context.Context) error {
	slog.Info("states: reconciler started", "interval", r.interval, "grace", r.grace)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.reconcile(ctx, "periodic")
		select {
		case <-ctx.Done():
			slog.Debug("states: reconciler stopped")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ReconcileOnce 扫一轮在途行并收敛，随后把 pending 行重新 dispatch（启动
// 恢复与对账恢复共用：崩溃残留的 pending、发布失败与对账 requeue 的行都在
// 此带走）。entry 启动时也会先调一轮，消化重启前未执行完的任务。
func (r *Reconciler) ReconcileOnce(ctx context.Context) {
	r.reconcile(ctx, "on_demand")
}

// reconcile 执行一轮对账。trigger 仅用于日志与观测（on_demand/periodic）。
func (r *Reconciler) reconcile(ctx context.Context, trigger string) {
	start := time.Now()
	slog.Debug("states: reconciliation round started", "trigger", trigger)

	// 只取空闲超过 grace 的行，再逐行按各自 timeout 判定，避免小预算任务被
	// 反复扫出。
	rows, err := r.repo.Inflight(ctx, time.Now().Add(-r.grace), reconcileBatch)
	if err != nil {
		slog.Error("states: failed to query inflight tasks", "trigger", trigger, "error", err)
		return
	}
	timedOut := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if !r.timedOut(row) {
			continue
		}
		timedOut++
		slog.Info("states: inflight task timed out",
			"task_id", row.Id, "resource", row.Resource, "phase", row.Phase,
			"timeout_ms", row.TimeoutMs, "updated_at", row.UpdatedAt)
		r.settle(ctx, row)
	}
	// types 为空 = 全部类型。在途行可能已超 reconcileBatch 截断，下一轮
	// 继续收敛。
	r.pub.DispatchPending(ctx, nil)

	if timedOut > 0 {
		slog.Info("states: reconciliation round completed",
			"trigger", trigger, "scanned", len(rows), "timed_out", timedOut,
			"duration_ms", time.Since(start).Milliseconds())
	} else {
		slog.Debug("states: reconciliation round completed",
			"trigger", trigger, "scanned", len(rows),
			"duration_ms", time.Since(start).Milliseconds())
	}
}

// timedOut 判断行是否超时：UpdatedAt 距今超过 max(timeout, 下限)+grace。
func (r *Reconciler) timedOut(row *TaskPersistent) bool {
	budget := time.Duration(row.TimeoutMs) * time.Millisecond
	if budget < minExecBudget {
		budget = minExecBudget
	}
	return time.Since(row.UpdatedAt) > budget+r.grace
}

// settle 收敛单行：按 queue backend 的任务状态分流。重派轮数不落表记账，
// 无次数上限——执行内重试由 queue 框架承担（Retries），重派仅在 Redis 状态
// 丢失（崩溃）后发生，重复执行由消费端幂等兜底。
func (r *Reconciler) settle(ctx context.Context, row *TaskPersistent) {
	sigUUID := rowSigUUID(row)
	state, err := r.backend.GetState(ctx, sigUUID)
	switch {
	case err == nil && state.IsSuccess():
		r.finish(ctx, row, TaskOutcomeSucceeded, "")
	case err == nil && state.IsFailure():
		r.finish(ctx, row, TaskOutcomeFailed, state.Error)
	case err != nil:
		slog.Warn("states: failed to query task state, requeueing",
			"task_id", row.Id, "uuid", sigUUID, "error", err)
		r.requeue(ctx, row, "backend_state_error")
	default:
		// 无终态记录（PENDING/STARTED/RETRY/不存在）：RETRY 语义上框架会
		// 重发，但 Redis 重启同样会丢 RETRY 记录，统一重派。
		slog.Info("states: no terminal task state found, requeueing",
			"task_id", row.Id, "uuid", sigUUID, "state", state.State)
		r.requeue(ctx, row, "missing_state")
	}
}

// requeue 把行退回 pending，失败只记日志：下一轮对账会再次扫到该行。
func (r *Reconciler) requeue(ctx context.Context, row *TaskPersistent, reason string) {
	if err := r.repo.Requeue(ctx, row.Id); err != nil {
		slog.Error("states: failed to requeue task",
			"task_id", row.Id, "reason", reason, "error", err)
	}
}

// finish 落终态并清理账本。
func (r *Reconciler) finish(ctx context.Context, row *TaskPersistent, outcome TaskOutcome, errSummary string) {
	if err := r.repo.MarkTerminal(ctx, row.Id, outcome, errSummary); err != nil {
		slog.Error("states: failed to mark task terminal",
			"task_id", row.Id, "resource", row.Resource, "outcome", outcome, "error", err)
		return
	}
	slog.Info("states: task settled",
		"task_id", row.Id, "resource", row.Resource, "outcome", outcome)
	if err := r.ledger.Delete(ctx, rowSigUUID(row)); err != nil {
		slog.Warn("states: failed to delete ledger entry",
			"task_id", row.Id, "uuid", rowSigUUID(row), "error", err)
	}
}
