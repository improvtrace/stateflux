package data

import (
	"context"
	"encoding/json"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpending"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskPendingsRepo 是 repository.TaskPendingsRepository 的 ent 实现（§3.1）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskPendingsRepo struct {
	data *Data
}

// NewTaskPendings 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskPendings(data *Data) repository.TaskPendingsRepository {
	return &taskPendingsRepo{data: data}
}

func (r *taskPendingsRepo) Create(ctx context.Context, tasks []repository.Pending) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskPendingCreate, 0, len(tasks))
	for _, t := range tasks {
		create := r.data.db.TaskPending.Create().
			SetID(t.TaskID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(int8(t.Priority)).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
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
	_, err := r.data.db.TaskPending.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskPendingsRepo) ListForPromotion(ctx context.Context, limit int) ([]repository.Pending, error) {
	rows, err := r.data.db.TaskPending.Query().
		Order(ent.Desc(taskpending.FieldPriority)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]repository.Pending, 0, len(rows))
	for _, row := range rows {
		out = append(out, pendingFromEnt(row))
	}
	return out, nil
}

func (r *taskPendingsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskPending.Delete().Where(taskpending.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskPendingsRepo) Get(ctx context.Context, taskID int64) (*repository.Pending, error) {
	row, err := r.data.db.TaskPending.Query().Where(taskpending.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	p := pendingFromEnt(row)
	return &p, nil
}

// pendingFromEnt 把 ent 实体投影为仓储实体（§5.1/§5.2）。
func pendingFromEnt(row *ent.TaskPending) repository.Pending {
	return repository.Pending{
		TaskID:         row.ID,
		Type:           row.Type,
		Operator:       row.Operator,
		Priority:       schema.Priority(row.Priority),
		Channel:        row.Channel,
		TimeoutMs:      row.TimeoutMs,
		MaxAttempts:    row.MaxAttempts,
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
