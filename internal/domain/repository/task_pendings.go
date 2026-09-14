package repository

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskPendingsRepository 定义 task_pendings 表的操作面（§3.1/§5.1/§5.2）。
type TaskPendingsRepository interface {
	// Create 批量写入待晋升任务（enqueue_tasks 的 pending 侧写入，§5.1）。
	Create(ctx context.Context, tasks []*ent.TaskPending) error
	// ListForPromotion 晋升扫描：按 priority DESC 取候选；纯前置条件（vpc/node/label/hash_bucket）
	// 由 Promoter 在候选集上判定（§5.2）。
	ListForPromotion(ctx context.Context, limit int) ([]*ent.TaskPending, error)
	// DeleteByIDs 晋升挪行后删除源行（与 task_schedulables 写入同事务，§5.2）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行（不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskPending, error)
}
