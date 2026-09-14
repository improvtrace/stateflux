package data

import (
	"context"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskprocessing"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskProcessingsRepo 是 repository.TaskProcessingsRepository 的 ent 实现（§3.1）。
type taskProcessingsRepo struct {
	client *ent.Client
}

// NewTaskProcessings 构造 task_processings 仓储；事务内复用时传入 tx.Client()。
func NewTaskProcessings(client *ent.Client) repository.TaskProcessingsRepository {
	return &taskProcessingsRepo{client: client}
}

func (r *taskProcessingsRepo) Create(ctx context.Context, tasks []*ent.TaskProcessing) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskProcessingCreate, 0, len(tasks))
	for _, t := range tasks {
		builders = append(builders, r.client.TaskProcessing.Create().
			SetID(t.ID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(t.Priority).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
			SetAttempts(t.Attempts).
			SetClaimedNode(t.ClaimedNode).
			SetIdempotencyKey(t.IdempotencyKey).
			SetCallback(t.Callback).
			SetParentTaskID(t.ParentTaskID).
			SetVpc(t.Vpc).
			SetNode(t.Node).
			SetBizRaceLabels(t.BizRaceLabels).
			SetBizRaceEntry(t.BizRaceEntry).
			SetBizGroup(t.BizGroup).
			SetBizBatchID(t.BizBatchID).
			SetCreatedAt(t.CreatedAt).
			SetUpdatedAt(t.UpdatedAt))
	}
	_, err := r.client.TaskProcessing.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskProcessingsRepo) ListExpired(ctx context.Context, deadline time.Time, limit int) ([]*ent.TaskProcessing, error) {
	return r.client.TaskProcessing.Query().
		Where(taskprocessing.UpdatedAtLTE(deadline)).
		Order(ent.Asc(taskprocessing.FieldUpdatedAt)).
		Limit(limit).
		All(ctx)
}

func (r *taskProcessingsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.client.TaskProcessing.Delete().Where(taskprocessing.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskProcessingsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskProcessing, error) {
	return r.client.TaskProcessing.Query().Where(taskprocessing.ID(taskID)).Only(ctx)
}
