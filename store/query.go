package store

import (
	"context"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/sdk"
	ent "github.com/improvtrace/stateflux/store/ent"
	"github.com/improvtrace/stateflux/store/ent/completed"
	"github.com/improvtrace/stateflux/store/ent/processing"
)

// GetResults 结果查询（§5.6）：先查 task_results（终态即命中返回），未命中查阶段表判断
// 在途状态。stages 覆盖全部请求 ID；结果与阶段都查不到时 stage = unknown。
func (s *pgStore) GetResults(ctx context.Context, taskIDs []int64) (map[int64]*sdk.TaskResult, map[int64]sdk.Stage, error) {
	results := make(map[int64]*sdk.TaskResult, len(taskIDs))
	stages := make(map[int64]sdk.Stage, len(taskIDs))
	if len(taskIDs) == 0 {
		return results, stages, nil
	}
	remaining := make([]int64, 0, len(taskIDs))
	for _, id := range taskIDs {
		stages[id] = sdk.StageUnknown // 缺省：任务不存在于任何阶段表
		r, ok, err := s.getResult(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			results[id] = r
			stages[id] = sdk.StageCompleted
		} else {
			remaining = append(remaining, id)
		}
	}
	if len(remaining) > 0 {
		found, err := s.locateStages(ctx, remaining)
		if err != nil {
			return nil, nil, err
		}
		for id, stage := range found {
			stages[id] = stage
		}
	}
	return results, stages, nil
}

// getResult 单条结果查询（ent API）。
func (s *pgStore) getResult(ctx context.Context, taskID int64) (*sdk.TaskResult, bool, error) {
	r, err := s.client.Result.Get(ctx, taskID)
	if ent.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: get result: %w", err)
	}
	return &sdk.TaskResult{
		TaskID:      r.ID,
		Outcome:     sdk.Outcome(r.Outcome),
		Attempt:     r.Attempt,
		Result:      r.Result,
		Error:       r.Error,
		CompletedAt: r.CompletedAt,
	}, true, nil
}

// locateStages 在途任务定位：pending → schedulable → processing（UNION ALL 一次查齐）。
func (s *pgStore) locateStages(ctx context.Context, ids []int64) (map[int64]sdk.Stage, error) {
	out := make(map[int64]sdk.Stage, len(ids))
	q := `SELECT id, 'pending' AS stage FROM pending_tasks WHERE id = ANY($1)
		UNION ALL SELECT id, 'schedulable' FROM schedulable_tasks WHERE id = ANY($1)
		UNION ALL SELECT id, 'processing' FROM processing_tasks WHERE id = ANY($1)`
	rows, err := s.db.QueryContext(ctx, q, pqArray(ids))
	if err != nil {
		return nil, fmt.Errorf("store: locate stages: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var stage string
		if err := rows.Scan(&id, &stage); err != nil {
			return nil, fmt.Errorf("store: scan stage: %w", err)
		}
		out[id] = sdk.Stage(stage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate stages: %w", err)
	}
	return out, nil
}

// ScanProcessing 对账扫描（§6.3 R1 候选）：updated_at 早于 cutoff 的 processing 任务。
func (s *pgStore) ScanProcessing(ctx context.Context, updatedBefore time.Time, limit int) ([]*sdk.Task, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.client.Processing.Query().
		Where(processing.UpdatedAtLTE(updatedBefore)).
		Order(ent.Asc(processing.FieldUpdatedAt)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: scan processing: %w", err)
	}
	out := make([]*sdk.Task, 0, len(rows))
	for _, p := range rows {
		out = append(out, &sdk.Task{
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
		})
	}
	return out, nil
}

// ExistsProcessing 判断哪些 task_id 仍在 processing（R2 幽灵清理用）。
func (s *pgStore) ExistsProcessing(ctx context.Context, taskIDs []int64) (map[int64]bool, error) {
	out := make(map[int64]bool, len(taskIDs))
	if len(taskIDs) == 0 {
		return out, nil
	}
	rows, err := s.client.Processing.Query().Where(processing.IDIn(taskIDs...)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: exists processing: %w", err)
	}
	for _, p := range rows {
		out[p.ID] = true
	}
	return out, nil
}

// ListDead 死信查询（§12.9）：outcome=dead，按 id 倒序分页；taskType 为空不过滤；
// cursor 为上一页返回的 next_cursor（0 表示从头）。
func (s *pgStore) ListDead(ctx context.Context, taskType string, limit int, cursor int64) ([]DeadTask, int64, error) {
	if limit <= 0 {
		limit = 100
	}
	q := s.client.Completed.Query().Where(completed.OutcomeEQ(string(sdk.OutcomeDead)))
	if taskType != "" {
		q = q.Where(completed.TypeEQ(taskType))
	}
	if cursor > 0 {
		q = q.Where(completed.IDLT(cursor))
	}
	rows, err := q.Order(ent.Desc(completed.FieldID)).Limit(limit).All(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("store: list dead: %w", err)
	}
	out := make([]DeadTask, 0, len(rows))
	var next int64
	for _, c := range rows {
		out = append(out, DeadTask{
			Task: sdk.Task{
				ID:             c.ID,
				Type:           c.Type,
				Priority:       sdk.Priority(c.Priority),
				ExecMode:       sdk.ExecMode(c.ExecMode),
				RunAt:          c.RunAt,
				TimeoutMS:      c.TimeoutMs,
				MaxAttempts:    c.MaxAttempts,
				Attempts:       c.Attempts,
				OwnerNode:      c.OwnerNode,
				Error:          c.Error,
				IdempotencyKey: c.IdempotencyKey,
				BatchID:        c.BatchID,
				ParentTaskID:   c.ParentTaskID,
				CreatedAt:      c.CreatedAt,
				UpdatedAt:      c.UpdatedAt,
			},
			Outcome:     sdk.Outcome(c.Outcome),
			Payload:     c.Payload,
			CompletedAt: c.CompletedAt,
		})
		next = c.ID
	}
	if len(rows) < limit {
		next = 0
	}
	return out, next, nil
}

// Redrive 死信重跑（§12.9）：按原 payload 复制创建全新任务实例（新雪花 ID、attempts 归零、
// 不自动重放）；回调规格随任务复制（重跑完成后回调链重新生效）。
func (s *pgStore) Redrive(ctx context.Context, taskIDs []int64) ([]int64, error) {
	if len(taskIDs) == 0 {
		return nil, ErrEmptyBatch
	}
	rows, err := s.client.Completed.Query().
		Where(completed.IDIn(taskIDs...), completed.OutcomeEQ(string(sdk.OutcomeDead))).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: load dead tasks: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	items := make([]sdk.NewTask, 0, len(rows))
	for _, c := range rows {
		var spec *sdk.CallbackSpec
		if len(c.Callback) > 0 {
			if parsed, perr := sdk.UnmarshalCallback(c.Callback); perr == nil {
				spec = parsed
			}
		}
		items = append(items, sdk.NewTask{
			Type:        c.Type,
			Payload:     c.Payload,
			Priority:    sdk.Priority(c.Priority),
			ExecMode:    sdk.ExecMode(c.ExecMode),
			RunAt:       time.Now(),
			TimeoutMS:   c.TimeoutMs,
			MaxAttempts: c.MaxAttempts,
			BatchID:     c.BatchID,
			Callback:    spec,
		})
	}
	created, err := s.CreatePending(ctx, items)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(created))
	for _, c := range created {
		ids = append(ids, c.TaskID)
	}
	return ids, nil
}

// LatestSuccessAt 工厂支撑（§5.7）：batch_id 前缀匹配的最新成功完成时间。
// next = last_success + period 现算，不持久化绝对时间点。
func (s *pgStore) LatestSuccessAt(ctx context.Context, batchPrefix string) (time.Time, bool, error) {
	c, err := s.client.Completed.Query().
		Where(
			completed.OutcomeEQ(string(sdk.OutcomeSucceeded)),
			completed.BatchIDHasPrefix(batchPrefix),
		).
		Order(ent.Desc(completed.FieldCompletedAt)).
		First(ctx)
	if ent.IsNotFound(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("store: latest success: %w", err)
	}
	return c.CompletedAt, true, nil
}

// HasDeadByBatchPrefix 工厂孤儿判定（§5.7）：前缀下存在 dead 任务即冻结条目、停止自动生成。
func (s *pgStore) HasDeadByBatchPrefix(ctx context.Context, batchPrefix string) (bool, error) {
	ok, err := s.client.Completed.Query().
		Where(
			completed.OutcomeEQ(string(sdk.OutcomeDead)),
			completed.BatchIDHasPrefix(batchPrefix),
		).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("store: has dead: %w", err)
	}
	return ok, nil
}
