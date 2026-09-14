package repository

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskCompletedsRepository 定义 task_completeds 表的操作面（§3.1/§5.5/§5.6）。
type TaskCompletedsRepository interface {
	// Create 批量写入终态历史（终态事务的 completed 侧，§5.5）。
	Create(ctx context.Context, tasks []*ent.TaskCompleted) error
	// CountByBizBatchID 按工厂批次计数：孤儿判定依据（§5.6）。
	CountByBizBatchID(ctx context.Context, bizBatchID string) (int, error)
	// ListDeadLetters 死信运维扫描：outcome=dead（§6.2 R3）。
	ListDeadLetters(ctx context.Context, limit int) ([]*ent.TaskCompleted, error)
	// Get 按 task_id 查询单行（不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskCompleted, error)
}
