package data

import (
	"context"
	"sort"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskcompleted"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpending"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskprocessing"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskschedulable"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// BatchProgress 统计某工厂批次的终态与在途数量（§5.6）：工厂据此判断上一批次是否仍在途，
// 避免重复生成造成堆积；在途为 0 且终态数量不足即孤儿批次的候选。
func (s *store) BatchProgress(ctx context.Context, bizBatchID string) (repository.BatchProgress, error) {
	var out repository.BatchProgress
	if bizBatchID == "" {
		return out, nil
	}
	c := s.txClient(ctx)
	completed, err := c.TaskCompleted.Query().Where(taskcompleted.BizBatchID(bizBatchID)).Count(ctx)
	if err != nil {
		return out, err
	}
	pending, err := c.TaskPending.Query().Where(taskpending.BizBatchID(bizBatchID)).Count(ctx)
	if err != nil {
		return out, err
	}
	schedulable, err := c.TaskSchedulable.Query().Where(taskschedulable.BizBatchID(bizBatchID)).Count(ctx)
	if err != nil {
		return out, err
	}
	processing, err := c.TaskProcessing.Query().Where(taskprocessing.BizBatchID(bizBatchID)).Count(ctx)
	if err != nil {
		return out, err
	}
	out.Completed = completed
	out.InFlight = pending + schedulable + processing
	return out, nil
}

// StaleBatches 返回创建时间早于 before 且仍未终态的在途批次 ID（工厂孤儿判定，§5.6）。
// 只覆盖记有 biz_batch_id 的行；结果去重并排序后截断到 limit。
func (s *store) StaleBatches(ctx context.Context, before time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	c := s.txClient(ctx)
	set := map[string]struct{}{}

	pending, err := c.TaskPending.Query().
		Where(taskpending.BizBatchIDNEQ(""), taskpending.CreatedAtLT(before)).
		Select(taskpending.FieldBizBatchID).
		GroupBy(taskpending.FieldBizBatchID).
		Strings(ctx)
	if err != nil {
		return nil, err
	}
	schedulable, err := c.TaskSchedulable.Query().
		Where(taskschedulable.BizBatchIDNEQ(""), taskschedulable.CreatedAtLT(before)).
		Select(taskschedulable.FieldBizBatchID).
		GroupBy(taskschedulable.FieldBizBatchID).
		Strings(ctx)
	if err != nil {
		return nil, err
	}
	processing, err := c.TaskProcessing.Query().
		Where(taskprocessing.BizBatchIDNEQ(""), taskprocessing.CreatedAtLT(before)).
		Select(taskprocessing.FieldBizBatchID).
		GroupBy(taskprocessing.FieldBizBatchID).
		Strings(ctx)
	if err != nil {
		return nil, err
	}
	for _, group := range [][]string{pending, schedulable, processing} {
		for _, id := range group {
			if id != "" {
				set[id] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
