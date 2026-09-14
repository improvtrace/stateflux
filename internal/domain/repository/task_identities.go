package repository

import (
	"context"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
)

// TaskIdentitiesRepository 定义 task_identities 表的操作面（§3.1/§5.1）。
type TaskIdentitiesRepository interface {
	// Put 批量幂等写入：键不存在则写入；dedupe 命中时返回账本中已有的原 task_id（§5.1）。
	// 返回值与入参一一对应：resolved 为每个键的最终 task_id，created 标记是否新建。
	Put(ctx context.Context, identities []*ent.TaskIdentity) ([]int64, []bool, error)
	// GetByIdempotencyKey 按幂等键查账本行（不存在返回 ent.NotFoundError）。
	GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*ent.TaskIdentity, error)
	// DeleteCreatedBefore 清理 dedupe 窗口外的账本行（窗口长度待定，§14），返回删除行数。
	DeleteCreatedBefore(ctx context.Context, cutoff time.Time) (int, error)
}
