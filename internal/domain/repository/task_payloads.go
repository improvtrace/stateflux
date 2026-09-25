package repository

import (
	"context"
	"encoding/json"
	"time"
)

// Payload 是 task_payloads 行的仓储实体（§3.1/§5.5）：与任务行分离存储的在途载荷，
// 终态合并入 task_results 后删除（§5.5）。
type Payload struct {
	TaskID    int64
	Payload   json.RawMessage
	CreatedAt time.Time
}

// TaskPayloadsRepository 定义 task_payloads 表的操作面（§3.1/§5.5）。
type TaskPayloadsRepository interface {
	// Create 批量写入在途 payload（enqueue_tasks 的 payload 侧写入，§5.1）。
	Create(ctx context.Context, payloads []Payload) error
	// Get 按 task_id 查询（终态合并时读取；不存在返回 Not Found 错误）。
	Get(ctx context.Context, taskID int64) (*Payload, error)
	// DeleteByIDs 终态合并入 task_results 后删除分离行（§5.5，同一事务）。
	DeleteByIDs(ctx context.Context, ids []int64) error
}
