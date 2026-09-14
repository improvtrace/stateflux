package repository

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskSchedulablesRepository 定义 task_schedulables 表的操作面（§3.1/§5.2）。
type TaskSchedulablesRepository interface {
	// Create 批量写入就绪任务（约束晋升的写入侧，§5.2）。
	Create(ctx context.Context, tasks []*ent.TaskSchedulable) error
	// ListForClaim 认领扫描：按 priority DESC 取候选（§5.2）。
	ListForClaim(ctx context.Context, limit int) ([]*ent.TaskSchedulable, error)
	// DeleteByIDs 认领挪行后删除源行（与 task_processings 写入同事务，§5.2）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行（不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskSchedulable, error)
}
