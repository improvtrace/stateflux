package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/factory"
)

// Enqueuer 把 factory 生成的任务写入 PG 创建路径（§5.1、§15.1#12）：雪花生成任务 ID，
// 经 repository.Store.Enqueue 在单事务内写身份账本、payload 与 pending。
type Enqueuer struct {
	store          repository.Store
	ids            *domain.Snowflake
	defaultChannel string
}

// NewEnqueuer 构造入队器。
func NewEnqueuer(store repository.Store, ids *domain.Snowflake, defaultChannel string) *Enqueuer {
	return &Enqueuer{store: store, ids: ids, defaultChannel: defaultChannel}
}

// Sink 返回 factory.Sink 适配器。
func (e *Enqueuer) Sink() factory.Sink {
	return func(ctx context.Context, t task.Task) error {
		_, _, err := e.Enqueue(ctx, t)
		return err
	}
}

// Enqueue 写入一个任务，返回最终 task_id 与是否新建。
func (e *Enqueuer) Enqueue(ctx context.Context, t task.Task) (int64, bool, error) {
	if e.store == nil || e.ids == nil {
		return 0, false, errors.New("biz: enqueuer not configured")
	}
	spec := t.Spec()
	if spec.Type == "" {
		return 0, false, errors.New("biz: task type must not be empty")
	}
	channel := spec.Channel
	if channel == "" {
		channel = e.defaultChannel
	}
	if channel == "" {
		return 0, false, errors.New("biz: task channel must not be empty")
	}
	taskID := e.ids.Next()
	key := spec.IdempotencyKey
	if key == "" {
		// 无业务幂等键时用自动键记录身份，保证同 ID 不会因重放重复入队。
		key = fmt.Sprintf("auto:%d", taskID)
	}
	now := time.Now()
	req := repository.EnqueueRequest{
		TaskID:         taskID,
		Type:           spec.Type,
		Operator:       spec.Operator,
		Priority:       priorityOf(spec.Priority),
		Channel:        channel,
		TimeoutMs:      defaultInt64(spec.TimeoutMS, 60_000),
		MaxAttempts:    int32(defaultInt64(int64(spec.MaxAttempts), 3)),
		IdempotencyKey: key,
		Callback:       json.RawMessage(spec.Callback),
		ParentTaskID:   spec.ParentTaskID,
		Vpc:            spec.VPC,
		Node:           spec.Node,
		Label:          spec.Label,
		HashBucket:     int16(spec.HashBucket),
		BizRaceLabels:  spec.BizRaceLabels,
		BizRaceEntry:   spec.BizRaceEntry,
		BizGroup:       spec.BizGroup,
		BizBatchID:     spec.BizBatchID,
		Payload:        payloadOrEmpty(t.Payload()),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	res, err := e.store.Enqueue(ctx, req)
	if err != nil {
		return 0, false, err
	}
	return res.TaskID, res.Created, nil
}

// priorityOf 归一化优先级：0 视为未设置，取默认 50；越界收敛到 [0,100]（§3.1）。
func priorityOf(p int32) schema.Priority {
	if p == 0 {
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

func defaultInt64(v, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

func payloadOrEmpty(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("{}")
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage("{}")
	}
	return json.RawMessage(quoted)
}
