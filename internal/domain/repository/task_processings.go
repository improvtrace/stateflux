package repository

import (
	"context"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskProcessingsRepository 定义 task_processings 表的操作面（§3.1/§5.2/§6.2）。
type TaskProcessingsRepository interface {
	// Create 批量写入在途任务（claim 挪行的写入侧，§5.2）。
	Create(ctx context.Context, tasks []*ent.TaskProcessing) error
	// ListExpired R1 对账扫描：取 updated_at 早于 deadline 的在途行（§6.2）。
	ListExpired(ctx context.Context, deadline time.Time, limit int) ([]*ent.TaskProcessing, error)
	// DeleteByIDs 终态/重置挪行后删除源行（§5.5）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行（不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskProcessing, error)
}
