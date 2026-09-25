package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Pending 是 task_pendings 行的仓储实体（§3.1/§5.1/§5.2）：与存储实现（ent）解耦的领域投影，
// 字段与表列一一对应。存储侧转换由 internal/domain/data 完成。
type Pending struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
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

// TaskPendingsRepository 定义 task_pendings 表的操作面（§3.1/§5.1/§5.2）。
type TaskPendingsRepository interface {
	// Create 批量写入待晋升任务（enqueue_tasks 的 pending 侧写入，§5.1）。
	Create(ctx context.Context, tasks []Pending) error
	// ListForPromotion 晋升扫描：按 priority DESC 取候选；纯前置条件（vpc/node/label/hash_bucket）
	// 由 Promoter 在候选集上判定（§5.2）。
	ListForPromotion(ctx context.Context, limit int) ([]Pending, error)
	// DeleteByIDs 晋升挪行后删除源行（与 task_schedulables 写入同事务，§5.2）。
	DeleteByIDs(ctx context.Context, ids []int64) error
	// Get 按 task_id 查询单行；不存在返回 Not Found 错误。
	Get(ctx context.Context, taskID int64) (*Pending, error)
}
