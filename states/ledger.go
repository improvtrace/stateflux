// Package states 收拢调度框架的全部数据库相关内容：
//   - task_persistent.go：调度事实源 task_persistents 的领域类型、repo 接口
//     与 repo 的 ent 实现；
//   - ent：ent schema（表结构的唯一来源，迁移文件由它经 Atlas 生成，见
//     migrations/）与生成码；
//   - client.go：连接构造与启动期就绪检查；表结构的迁移见 migrations/；
//   - ledger.go / reconcile.go：在途视图——ledger 是发布侧的加速索引，
//     reconciler 把在途超时的表行收敛到终态或回 pending，并把 pending 行
//     重派到队列（启动与对账恢复通道，经根包 publisher）。
//
// 事实源始终是 task_persistents 表，本包的 Redis 数据丢了均可由对账重建。
package states

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// defaultLedgerKey 是记账 hash 的缺省 key。
const defaultLedgerKey = "schedule_stable:ledger"

// ErrNotFound 表示账本中无该 Signature 的记录。
var ErrNotFound = errors.New("states: no ledger entry")

// SignatureUUID 生成 task_persistents 行对应的 Signature.UUID：task_{id}。
// 同一行重派复用同一 UUID（与发布侧、对账侧共用此协议），执行框架按
// UUID 覆盖写状态，重派不会残留旧状态。
func SignatureUUID(taskID int64) string {
	return fmt.Sprintf("task_%d", taskID)
}

// Ledger 记录已发布 Signature.UUID → taskID 的映射（Redis hash）。
// 仅是加速索引：Redis 无持久化，重启即丢，对账以表为准，不依赖本结构存活。
type Ledger struct {
	rdb redis.UniversalClient
	key string
}

// NewLedger 构造记账，key 空时取缺省值。
func NewLedger(rdb redis.UniversalClient, key string) *Ledger {
	if key == "" {
		key = defaultLedgerKey
	}
	return &Ledger{rdb: rdb, key: key}
}

// Record 记录一条映射，覆盖旧值。
func (l *Ledger) Record(ctx context.Context, sigUUID string, taskID int64) error {
	return l.rdb.HSet(ctx, l.key, sigUUID, taskID).Err()
}

// TaskID 查询映射，无记录返回 ErrNotFound。
func (l *Ledger) TaskID(ctx context.Context, sigUUID string) (int64, error) {
	v, err := l.rdb.HGet(ctx, l.key, sigUUID).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("states: invalid ledger value %q: %w", v, err)
	}
	return id, nil
}

// Delete 清除终态任务的映射。
func (l *Ledger) Delete(ctx context.Context, sigUUIDs ...string) error {
	if len(sigUUIDs) == 0 {
		return nil
	}
	return l.rdb.HDel(ctx, l.key, sigUUIDs...).Err()
}

// rowSigUUID 返回表行对应的 Signature.UUID。
func rowSigUUID(row *TaskPersistent) string {
	return SignatureUUID(row.Id)
}
