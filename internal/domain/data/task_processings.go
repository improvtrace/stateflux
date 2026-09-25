package data

import (
	"context"
	"encoding/json"
	"time"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskprocessing"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskProcessingsRepo 是 repository.TaskProcessingsRepository 的 ent 实现（§3.1）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskProcessingsRepo struct {
	data *Data
}

// NewTaskProcessings 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskProcessings(data *Data) repository.TaskProcessingsRepository {
	return &taskProcessingsRepo{data: data}
}

func (r *taskProcessingsRepo) Create(ctx context.Context, tasks []repository.Processing) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskProcessingCreate, 0, len(tasks))
	for _, t := range tasks {
		create := r.data.db.TaskProcessing.Create().
			SetID(t.TaskID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(int8(t.Priority)).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
			SetAttempts(t.Attempt).
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
	_, err := r.data.db.TaskProcessing.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskProcessingsRepo) ListExpired(ctx context.Context, deadline time.Time, limit int) ([]repository.Processing, error) {
	rows, err := r.data.db.TaskProcessing.Query().
		Where(taskprocessing.UpdatedAtLT(deadline)).
		Order(ent.Asc(taskprocessing.FieldUpdatedAt)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]repository.Processing, 0, len(rows))
	for _, row := range rows {
		out = append(out, processingFromEnt(row))
	}
	return out, nil
}

func (r *taskProcessingsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskProcessing.Delete().Where(taskprocessing.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskProcessingsRepo) Get(ctx context.Context, taskID int64) (*repository.Processing, error) {
	row, err := r.data.db.TaskProcessing.Query().Where(taskprocessing.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	p := processingFromEnt(row)
	return &p, nil
}

// processingFromEnt 把 ent 实体投影为仓储实体（§5.2）。
func processingFromEnt(row *ent.TaskProcessing) repository.Processing {
	return repository.Processing{
		TaskID:         row.ID,
		Type:           row.Type,
		Operator:       row.Operator,
		Priority:       schema.Priority(row.Priority),
		Channel:        row.Channel,
		TimeoutMs:      row.TimeoutMs,
		MaxAttempts:    row.MaxAttempts,
		Attempt:        row.Attempts,
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
