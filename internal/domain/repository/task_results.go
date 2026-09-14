package repository

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskResultsRepository 定义 task_results 表的操作面（§3.1/§5.5）。
type TaskResultsRepository interface {
	// Create 批量墓碑写入：task_id 主键冲突即重复归集，对应行返回 false 跳过（§5.5）。
	// 返回值与入参一一对应，标记每行是否新建。
	Create(ctx context.Context, results []*ent.TaskResult) ([]bool, error)
	// Get 按 task_id 查询终态结果（业务侧结果查询入口，§5.1；不存在返回 ent.NotFoundError）。
	Get(ctx context.Context, taskID int64) (*ent.TaskResult, error)
}
