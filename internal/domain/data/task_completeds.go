package data

import (
	"context"
	"encoding/json"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskcompleted"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskCompletedsRepo 是 repository.TaskCompletedsRepository 的 ent 实现（§3.1）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskCompletedsRepo struct {
	data *Data
}

// NewTaskCompleteds 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskCompleteds(data *Data) repository.TaskCompletedsRepository {
	return &taskCompletedsRepo{data: data}
}

func (r *taskCompletedsRepo) Create(ctx context.Context, tasks []repository.Completed) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskCompletedCreate, 0, len(tasks))
	for _, t := range tasks {
		create := r.data.db.TaskCompleted.Create().
			SetID(t.TaskID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(int8(t.Priority)).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
			SetAttempts(t.Attempts).
			SetClaimedNode(t.ClaimedNode).
			SetError(t.Error).
			SetIdempotencyKey(t.IdempotencyKey).
			SetParentTaskID(t.ParentTaskID).
			SetVpc(t.Vpc).
			SetNode(t.Node).
			SetLabel(t.Label).
			SetBizRaceLabels(t.BizRaceLabels).
			SetBizRaceEntry(t.BizRaceEntry).
			SetBizGroup(t.BizGroup).
			SetBizBatchID(t.BizBatchID).
			SetCreatedAt(t.CreatedAt).
			SetUpdatedAt(t.UpdatedAt).
			SetOutcome(int8(t.Outcome)).
			SetCompletedAt(t.CompletedAt)
		if len(t.Callback) > 0 {
			create = create.SetCallback(jsontext.Value(t.Callback))
		}
		builders = append(builders, create)
	}
	_, err := r.data.db.TaskCompleted.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskCompletedsRepo) CountByBizBatchID(ctx context.Context, bizBatchID string) (int, error) {
	return r.data.db.TaskCompleted.Query().Where(taskcompleted.BizBatchID(bizBatchID)).Count(ctx)
}

func (r *taskCompletedsRepo) ListDeadLetters(ctx context.Context, limit int) ([]repository.Completed, error) {
	rows, err := r.data.db.TaskCompleted.Query().
		Where(taskcompleted.OutcomeEQ(int8(schema.OutcomeDead))).
		Order(ent.Desc(taskcompleted.FieldCompletedAt)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]repository.Completed, 0, len(rows))
	for _, row := range rows {
		out = append(out, completedFromEnt(row))
	}
	return out, nil
}

func (r *taskCompletedsRepo) Get(ctx context.Context, taskID int64) (*repository.Completed, error) {
	row, err := r.data.db.TaskCompleted.Query().Where(taskcompleted.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	c := completedFromEnt(row)
	return &c, nil
}

// completedFromEnt 把 ent 实体投影为仓储实体（§5.5）。
func completedFromEnt(row *ent.TaskCompleted) repository.Completed {
	return repository.Completed{
		TaskID:         row.ID,
		Type:           row.Type,
		Operator:       row.Operator,
		Priority:       schema.Priority(row.Priority),
		Channel:        row.Channel,
		TimeoutMs:      row.TimeoutMs,
		MaxAttempts:    row.MaxAttempts,
		Attempts:       row.Attempts,
		ClaimedNode:    row.ClaimedNode,
		Error:          row.Error,
		IdempotencyKey: row.IdempotencyKey,
		Callback:       json.RawMessage(row.Callback),
		ParentTaskID:   row.ParentTaskID,
		Vpc:            row.Vpc,
		Node:           row.Node,
		Label:          row.Label,
		BizRaceLabels:  row.BizRaceLabels,
		BizRaceEntry:   row.BizRaceEntry,
		BizGroup:       row.BizGroup,
		BizBatchID:     row.BizBatchID,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
		Outcome:        schema.Outcome(row.Outcome),
		CompletedAt:    row.CompletedAt,
	}
}
