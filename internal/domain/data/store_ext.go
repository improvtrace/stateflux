package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpayload"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskprocessing"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskschedulable"
)

// GetPayload 读取在途任务 payload（构造 TaskMessage，§3.3）。payload 缺失时返回 ent.NotFoundError；
// 调度周期通常已保证在途任务必有 payload，缺失只会让该轮跳过并由 R1 兜底。
func (s *store) GetPayload(ctx context.Context, taskID int64) ([]byte, error) {
	p, err := s.txClient(ctx).TaskPayload.Query().Where(taskpayload.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	return p.Payload, nil
}

// Requeue 是可重试失败的回退路径（§5.5）：processing → schedulable，attempts/claimed_node
// 原样保留（重试不递增 attempt，下次 claim 才 +1），并在同事务内删除 processing 源行。
// 行不存在返回 false（可能已被 R1 重置或已终态），不产生副作用。
func (s *store) Requeue(ctx context.Context, taskID int64) (bool, error) {
	moved := false
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		c := s.txClient(ctx)
		p, err := c.TaskProcessing.Query().Where(taskprocessing.ID(taskID)).Only(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				return nil
			}
			return err
		}
		create := c.TaskSchedulable.Create().
			SetID(p.ID).
			SetType(p.Type).
			SetOperator(p.Operator).
			SetPriority(p.Priority).
			SetChannel(p.Channel).
			SetTimeoutMs(p.TimeoutMs).
			SetMaxAttempts(p.MaxAttempts).
			SetAttempts(p.Attempts).
			SetClaimedNode(p.ClaimedNode).
			SetIdempotencyKey(p.IdempotencyKey).
			SetParentTaskID(p.ParentTaskID).
			SetVpc(p.Vpc).
			SetNode(p.Node).
			SetLabel(p.Label).
			SetHashBucket(p.HashBucket).
			SetBizRaceLabels(p.BizRaceLabels).
			SetBizRaceEntry(p.BizRaceEntry).
			SetBizGroup(p.BizGroup).
			SetBizBatchID(p.BizBatchID).
			SetCreatedAt(p.CreatedAt).
			SetUpdatedAt(p.UpdatedAt)
		if len(p.Callback) > 0 {
			create = create.SetCallback(p.Callback)
		}
		if err := create.Exec(ctx); err != nil {
			return err
		}
		if _, err := c.TaskProcessing.Delete().Where(taskprocessing.ID(taskID)).Exec(ctx); err != nil {
			return err
		}
		moved = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return moved, nil
}

// 保证 taskschedulable 包被引用（构建器类型断言的可读性锚点）。
var _ = taskschedulable.FieldID
