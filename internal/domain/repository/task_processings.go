package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Processing 是 task_processings 行的仓储实体（§3.1/§5.2）：只携带调度、分发与观测所需字段。
// Attempt 即执行 fence：本次认领后的 attempts，只在 claim 时递增（§5.5/§6.2）。Claim 与
// GetProcessing 都返回它。
type Processing struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
	Attempt        int64
	ClaimedNode    string
	IdempotencyKey string
	Callback       json.RawMessage
	ParentTaskID   int64
	Vpc            string
	Node           string
	Label          string
	HashBucket     int16
	BizRaceLabels  []string
	BizRaceEntry   string
	BizGroup       string
	BizBatchID     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Claimed 是 Processing 的别名，强调该行来自 claim 挪行（§5.2）。
type Claimed = Processing

// TaskProcessingsRepository 定义 task_processings 表的操作面（§3.1/§5.2/§6.2）。
type TaskProcessingsRepository interface {
	// Create 批量写入在途任务（claim 挪行的写入侧，§5.2）。Attempt 为写入的 attempts 值
	// （已含本次 +1）。
	Create(ctx context.Context, tasks []Processing) error
	// ListExpired R1 对账扫描：取 updated_at 早于 deadline 的在途行（§6.2）。
	ListExpired(ctx context.Context, deadline time.Time, limit int) ([]Processing, error)
	// DeleteByIDs 终态/重置挪行后删除源行（§5.5）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行；不存在返回 Not Found 错误。
	Get(ctx context.Context, taskID int64) (*Processing, error)
}
