package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Result 是 task_results 行的仓储实体（§5.5）：终态墓碑。payload 与业务结果在终态事务内
// 合并入行；task_id 主键冲突即重复归集（§5.5）。
type Result struct {
	TaskID      int64
	Outcome     schema.Outcome
	Attempt     int64
	Payload     json.RawMessage
	Result      json.RawMessage
	Error       string
	CompletedAt time.Time
}

// TaskResultsRepository 定义 task_results 表的操作面（§3.1/§5.5）。
type TaskResultsRepository interface {
	// Create 批量墓碑写入：task_id 主键冲突即重复归集，对应行返回 false 跳过（§5.5）。
	// 返回值与入参一一对应，标记每行是否新建。
	Create(ctx context.Context, results []Result) ([]bool, error)
	// Get 按 task_id 查询终态结果（业务侧结果查询入口，§5.1；不存在返回 Not Found 错误）。
	Get(ctx context.Context, taskID int64) (*Result, error)
}
