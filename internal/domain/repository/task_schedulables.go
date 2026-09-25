package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Schedulable 是 task_schedulables 行的仓储实体（§3.1/§5.2）：公共段 + 认领阶段字段。
// Attempts 是已消耗的认领次数（本次认领前）——认领时才 +1 并写入 processing（§5.2）。
type Schedulable struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
	Attempts       int64
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

// TaskSchedulablesRepository 定义 task_schedulables 表的操作面（§3.1/§5.2）。
type TaskSchedulablesRepository interface {
	// Create 批量写入就绪任务（约束晋升的写入侧，§5.2）。
	Create(ctx context.Context, tasks []Schedulable) error
	// ListForClaim 认领扫描：按 priority DESC 取候选（§5.2）。
	ListForClaim(ctx context.Context, limit int) ([]Schedulable, error)
	// DeleteByIDs 认领挪行后删除源行（与 task_processings 写入同事务，§5.2）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行；不存在返回 Not Found 错误。
	Get(ctx context.Context, taskID int64) (*Schedulable, error)
}
