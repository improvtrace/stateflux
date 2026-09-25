package data

import (
	"context"
	"encoding/json"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskschedulable"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskSchedulablesRepo 是 repository.TaskSchedulablesRepository 的 ent 实现（§3.1）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskSchedulablesRepo struct {
	data *Data
}

// NewTaskSchedulables 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskSchedulables(data *Data) repository.TaskSchedulablesRepository {
	return &taskSchedulablesRepo{data: data}
}

func (r *taskSchedulablesRepo) Create(ctx context.Context, tasks []repository.Schedulable) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskSchedulableCreate, 0, len(tasks))
	for _, t := range tasks {
		create := r.data.db.TaskSchedulable.Create().
			SetID(t.TaskID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(int8(t.Priority)).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
			SetAttempts(t.Attempts).
			SetClaimedNode(t.ClaimedNode).
			SetIdempotencyKey(t.IdempotencyKey).
			SetParentTaskID(t.ParentTaskID).
			SetVpc(t.Vpc).
			SetNode(t.Node).
			SetLabel(t.Label).
			SetHashBucket(t.HashBucket).
			SetBizRaceLabels(t.BizRaceLabels).
			SetBizRaceEntry(t.BizRaceEntry).
			SetBizGroup(t.BizGroup).
			SetBizBatchID(t.BizBatchID).
			SetCreatedAt(t.CreatedAt).
			SetUpdatedAt(t.UpdatedAt)
		if len(t.Callback) > 0 {
			create = create.SetCallback(jsontext.Value(t.Callback))
		}
		builders = append(builders, create)
	}
	_, err := r.data.db.TaskSchedulable.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskSchedulablesRepo) ListForClaim(ctx context.Context, limit int) ([]repository.Schedulable, error) {
	rows, err := r.data.db.TaskSchedulable.Query().
		Order(ent.Desc(taskschedulable.FieldPriority)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]repository.Schedulable, 0, len(rows))
	for _, row := range rows {
		out = append(out, schedulableFromEnt(row))
	}
	return out, nil
}

func (r *taskSchedulablesRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskSchedulable.Delete().Where(taskschedulable.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskSchedulablesRepo) Get(ctx context.Context, taskID int64) (*repository.Schedulable, error) {
	row, err := r.data.db.TaskSchedulable.Query().Where(taskschedulable.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	s := schedulableFromEnt(row)
	return &s, nil
}

// schedulableFromEnt 把 ent 实体投影为仓储实体（§5.2）。
func schedulableFromEnt(row *ent.TaskSchedulable) repository.Schedulable {
	return repository.Schedulable{
		TaskID:         row.ID,
		Type:           row.Type,
		Operator:       row.Operator,
		Priority:       schema.Priority(row.Priority),
		Channel:        row.Channel,
		TimeoutMs:      row.TimeoutMs,
		MaxAttempts:    row.MaxAttempts,
		Attempts:       row.Attempts,
		ClaimedNode:    row.ClaimedNode,
		IdempotencyKey: row.IdempotencyKey,
		Callback:       json.RawMessage(row.Callback),
		ParentTaskID:   row.ParentTaskID,
		Vpc:            row.Vpc,
		Node:           row.Node,
		Label:          row.Label,
		HashBucket:     row.HashBucket,
		BizRaceLabels:  row.BizRaceLabels,
		BizRaceEntry:   row.BizRaceEntry,
		BizGroup:       row.BizGroup,
		BizBatchID:     row.BizBatchID,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	}
}
