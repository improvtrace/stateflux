package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Completed 是 task_completeds 行的仓储实体（§3.1/§5.5/§5.6）：公共段 + 认领与终态字段。
// 终态行不再携带调度目标属性 hash_bucket（终态后不再分发）。
type Completed struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
	Attempts       int64
	ClaimedNode    string
	Error          string
	IdempotencyKey string
	Callback       json.RawMessage
	ParentTaskID   int64
	Vpc            string
	Node           string
	Label          string
	BizRaceLabels  []string
	BizRaceEntry   string
	BizGroup       string
	BizBatchID     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Outcome        schema.Outcome
	CompletedAt    time.Time
}

// TaskCompletedsRepository 定义 task_completeds 表的操作面（§3.1/§5.5/§5.6）。
type TaskCompletedsRepository interface {
	// Create 批量写入终态历史（终态事务的 completed 侧，§5.5）。
	Create(ctx context.Context, tasks []Completed) error
	// CountByBizBatchID 按工厂批次计数：孤儿判定依据（§5.6）。
	CountByBizBatchID(ctx context.Context, bizBatchID string) (int, error)
	// ListDeadLetters 死信运维扫描：outcome=dead（§6.2 R3）。
	ListDeadLetters(ctx context.Context, limit int) ([]Completed, error)
	// Get 按 task_id 查询单行；不存在返回 Not Found 错误。
	Get(ctx context.Context, taskID int64) (*Completed, error)
}
