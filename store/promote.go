package store

import (
	"context"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/sdk"
	ent "github.com/improvtrace/stateflux/store/ent"
	"github.com/improvtrace/stateflux/store/ent/pending"
)

// Promote 约束晋升（§5.2）：扫描 pending（run_at 到期，priority DESC + run_at）→
// 内置约束（per-type 全局并发上限，按 processing_tasks 在途计数判定）+ 业务约束钩子评估 →
// 批量挪入 schedulable（单条 CTE 挪行）。约束未通过的任务留在 pending，等待下轮评估，
// 不占用认领扫描。
func (s *pgStore) Promote(ctx context.Context, opts PromoteOptions) (PromoteStats, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	// 扫描到期 pending（常规读走 ent 类型安全 API；晋升挪行走原生 SQL）。
	rows, err := s.client.Pending.Query().
		Where(pending.RunAtLTE(now)).
		Order(ent.Desc(pending.FieldPriority), ent.Asc(pending.FieldRunAt), ent.Asc(pending.FieldID)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return PromoteStats{}, fmt.Errorf("store: scan pending: %w", err)
	}
	stats := PromoteStats{Scanned: len(rows), BlockedPreconditionByType: map[string]int{}}
	if len(rows) == 0 {
		return stats, nil
	}

	// 内置约束：per-type 在途计数（§5.2）。
	typeSet := make(map[string]struct{}, len(rows))
	types := make([]string, 0, len(rows))
	candidates := make([]*sdk.Task, 0, len(rows))
	for _, p := range rows {
		t := pendingRowToTask(p)
		candidates = append(candidates, t)
		if _, ok := typeSet[t.Type]; !ok {
			typeSet[t.Type] = struct{}{}
			types = append(types, t.Type)
		}
	}
	inflight := make(map[string]int, len(types))
	if len(opts.TypeConcurrency) > 0 && len(types) > 0 {
		counts, err := s.countProcessingByType(ctx, types)
		if err != nil {
			return stats, err
		}
		inflight = counts
	}

	// 业务约束钩子（§5.2）：晋升前由框架调用。
	eligible := make([]*sdk.Task, 0, len(candidates))
	for _, t := range candidates {
		if cap, ok := opts.TypeConcurrency[t.Type]; ok && cap > 0 && inflight[t.Type] >= cap {
			stats.BlockedConcurrency++
			continue
		}
		if pre, ok := opts.Preconditions[t.Type]; ok && pre != nil {
			pass, err := pre(ctx, t)
			if err != nil {
				// 钩子报错按未通过处理（留在 pending），不阻断晋升批次。
				s.log.WarnContext(ctx, "store: precondition hook error",
					"type", t.Type, "task_id", t.ID, "err", err)
				stats.BlockedPrecondition++
				stats.BlockedPreconditionByType[t.Type]++
				continue
			}
			if !pass {
				stats.BlockedPrecondition++
				stats.BlockedPreconditionByType[t.Type]++
				continue
			}
		}
		eligible = append(eligible, t)
	}
	if len(eligible) == 0 {
		return stats, nil
	}

	// 批量挪行：DELETE pending RETURNING → INSERT schedulable，单条 CTE 原子完成。
	ids := make([]int64, 0, len(eligible))
	for _, t := range eligible {
		ids = append(ids, t.ID)
	}
	promoted, err := s.movePendingToSchedulable(ctx, ids)
	if err != nil {
		return stats, err
	}
	stats.Promoted = promoted
	return stats, nil
}

// pendingRowToTask ent 行 → sdk.Task（不含 payload；晋升不需要）。
func pendingRowToTask(p *ent.Pending) *sdk.Task {
	t := &sdk.Task{
		ID:             p.ID,
		Type:           p.Type,
		Priority:       sdk.Priority(p.Priority),
		ExecMode:       sdk.ExecMode(p.ExecMode),
		RunAt:          p.RunAt,
		TimeoutMS:      p.TimeoutMs,
		MaxAttempts:    p.MaxAttempts,
		Attempts:       p.Attempts,
		OwnerNode:      p.OwnerNode,
		Error:          p.Error,
		IdempotencyKey: p.IdempotencyKey,
		BatchID:        p.BatchID,
		ParentTaskID:   p.ParentTaskID,
		CreatedAt:      p.CreatedAt,
		UpdatedAt:      p.UpdatedAt,
	}
	if len(p.Callback) > 0 {
		if spec, err := sdk.UnmarshalCallback(p.Callback); err == nil {
			t.Callback = spec
		}
	}
	return t
}

// countProcessingByType processing 在途计数（内置并发约束判定依据，§5.2）。
func (s *pgStore) countProcessingByType(ctx context.Context, types []string) (map[string]int, error) {
	sb := newQueryBuilder()
	sb.WriteString(`SELECT type, count(*)::int FROM processing_tasks WHERE type = ANY(`)
	sb.writeCast(pqArray(types), "text[]")
	sb.WriteString(`) GROUP BY type`)
	rows, err := s.db.QueryContext(ctx, sb.String(), sb.args...)
	if err != nil {
		return nil, fmt.Errorf("store: count processing by type: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var typ string
		var n int
		if err := rows.Scan(&typ, &n); err != nil {
			return nil, fmt.Errorf("store: scan type count: %w", err)
		}
		out[typ] = n
	}
	return out, rows.Err()
}

// movePendingToSchedulable 批量挪行：pending → schedulable（单条 CTE，原子完成）。
func (s *pgStore) movePendingToSchedulable(ctx context.Context, ids []int64) (int, error) {
	sb := newQueryBuilder()
	sb.WriteString(`WITH moved AS (
		DELETE FROM pending_tasks WHERE id = ANY(`)
	sb.writeCast(pqArray(ids), "bigint[]")
	sb.WriteString(`)
		RETURNING ` + stageColumns + `
	), ins AS (
		INSERT INTO schedulable_tasks (` + stageColumns + `)
		SELECT ` + stageColumns + ` FROM moved
		ON CONFLICT (id) DO NOTHING
		RETURNING id
	)
	SELECT count(*)::int FROM ins`)
	var n int
	if err := s.db.QueryRowContext(ctx, sb.String(), sb.args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: move pending to schedulable: %w", err)
	}
	return n, nil
}
