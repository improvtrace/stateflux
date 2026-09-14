package repository

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskPayloadsRepository 定义 task_payloads 表的操作面（§3.1/§5.5）。
type TaskPayloadsRepository interface {
	// Create 批量写入在途 payload（enqueue_tasks 的 payload 侧写入，§5.1）。
	Create(ctx context.Context, payloads []*ent.TaskPayload) error
	// Get 按 task_id 查询（终态合并时读取；不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskPayload, error)
	// DeleteByIDs 终态合并入 task_results 后删除分离行（§5.5，同一事务）。
	DeleteByIDs(ctx context.Context, ids []int64) error
}
