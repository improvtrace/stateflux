package data

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskcompleted"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// 派生任务的缺省字段（与 enqueue 缺省一致，§3.1）。
const (
	derivedDefaultTimeoutMS   = 60000
	derivedDefaultMaxAttempts = 3
)

// deriveCallback 在终态事务内派生回调任务（§5.6）：读取 processing 行的 callback 规格，
// 按 outcome 选 OnSuccess/OnError，以 `parent:attempt:outcome` 为幂等键写入 pending。
//
// 幂等：身份账本 ON CONFLICT DO NOTHING 保证同一 parent:attempt:outcome 只派生一次；
// 深度：沿 task_completeds.parent_task_id 上溯，超过 domain.MaxCallbackDepth 不再派生。
// 回调 JSON 非法时不派生（不阻塞终态提交，避免毒丸任务卡死终态）。
func deriveCallback(ctx context.Context, c *ent.Client, nextID func() int64, proc *ent.TaskProcessing, outcome schema.Outcome, completedAt time.Time) error {
	if nextID == nil || len(proc.Callback) == 0 {
		return nil
	}
	spec, err := domain.UnmarshalCallback(proc.Callback)
	if err != nil || spec == nil {
		return nil
	}
	derived := spec.Derive(outcome == schema.OutcomeSucceeded)
	if derived == nil || derived.Type == "" {
		return nil
	}
	depth, err := callbackDepth(ctx, c, proc.ID)
	if err != nil {
		return err
	}
	if depth >= domain.MaxCallbackDepth {
		return nil
	}

	taskID := nextID()
	key := derived.IdempotencyKey
	if key == "" {
		// 设计指定：parent:attempt:outcome（§5.6）。
		key = fmt.Sprintf("%d:%d:%d", proc.ID, proc.Attempts, int8(outcome))
	}
	res, err := c.ExecContext(ctx, upsertIdentitySQL, taskID, key, completedAt)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// 并发/重复派生：账本已有同一幂等键，跳过（§5.6）。
		return nil
	}

	payload := jsontext.Value(derived.Payload)
	if len(payload) == 0 {
		payload = jsontext.Value("null")
	}
	if err := c.TaskPayload.Create().
		SetID(taskID).
		SetPayload(payload).
		SetCreatedAt(completedAt).
		Exec(ctx); err != nil {
		return err
	}

	create := c.TaskPending.Create().
		SetID(taskID).
		SetType(derived.Type).
		SetOperator(derived.Operator).
		SetPriority(int8(derivedPriority(derived.Priority))).
		SetChannel(derivedChannel(derived.Channel, proc.Channel)).
		SetTimeoutMs(derivedInt64(derived.TimeoutMS, derivedDefaultTimeoutMS)).
		SetMaxAttempts(int32(derivedInt64(int64(derived.MaxAttempts), derivedDefaultMaxAttempts))).
		SetIdempotencyKey(key).
		SetParentTaskID(proc.ID).
		SetVpc(derived.VPC).
		SetNode(derived.Node).
		SetLabel(derived.Label).
		SetHashBucket(int16(derived.HashBucket)).
		SetBizRaceLabels(derived.BizRaceLabels).
		SetBizRaceEntry(derived.BizRaceEntry).
		SetBizGroup(derived.BizGroup).
		SetBizBatchID(derived.BizBatchID).
		SetCreatedAt(completedAt).
		SetUpdatedAt(completedAt)
	if cb, cerr := domain.MarshalCallback(derived.Callback); cerr == nil && len(cb) > 0 {
		create = create.SetCallback(jsontext.Value(cb))
	}
	return create.Exec(ctx)
}

// callbackDepth 返回某个已完成任务到根任务的祖先层数（根为 0）。链条超出上限即提前返回。
func callbackDepth(ctx context.Context, c *ent.Client, taskID int64) (int, error) {
	depth := 0
	cur := taskID
	for depth <= domain.MaxCallbackDepth {
		row, err := c.TaskCompleted.Query().Where(taskcompleted.ID(cur)).Only(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				return depth, nil
			}
			return 0, err
		}
		if row.ParentTaskID == 0 {
			return depth, nil
		}
		cur = row.ParentTaskID
		depth++
	}
	return depth, nil
}

func derivedPriority(p int32) schema.Priority {
	if p <= 0 {
		return schema.DefaultPriority
	}
	if p < int32(schema.MinPriority) {
		return schema.MinPriority
	}
	if p > int32(schema.MaxPriority) {
		return schema.MaxPriority
	}
	return schema.Priority(p)
}

func derivedChannel(channel, fallback string) string {
	if channel != "" {
		return channel
	}
	return fallback
}

func derivedInt64(v, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

// 保证 encoding/json 被引用（CallbackTask.Payload 的显式类型）。
var _ = json.RawMessage(nil)
