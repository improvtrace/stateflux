package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskschedulable"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskSchedulablesRepo 是 repository.TaskSchedulablesRepository 的 ent 实现（§3.1）。
type taskSchedulablesRepo struct {
	client *ent.Client
}

// NewTaskSchedulables 构造 task_schedulables 仓储；事务内复用时传入 tx.Client()。
func NewTaskSchedulables(client *ent.Client) repository.TaskSchedulablesRepository {
	return &taskSchedulablesRepo{client: client}
}

func (r *taskSchedulablesRepo) Create(ctx context.Context, tasks []*ent.TaskSchedulable) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskSchedulableCreate, 0, len(tasks))
	for _, t := range tasks {
		builders = append(builders, r.client.TaskSchedulable.Create().
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
			SetLabel(t.Label).
			SetHashBucket(t.HashBucket).
			SetBizRaceLabels(t.BizRaceLabels).
			SetBizRaceEntry(t.BizRaceEntry).
			SetBizGroup(t.BizGroup).
			SetBizBatchID(t.BizBatchID).
			SetCreatedAt(t.CreatedAt).
			SetUpdatedAt(t.UpdatedAt))
	}
	_, err := r.client.TaskSchedulable.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskSchedulablesRepo) ListForClaim(ctx context.Context, limit int) ([]*ent.TaskSchedulable, error) {
	return r.client.TaskSchedulable.Query().
		Order(ent.Desc(taskschedulable.FieldPriority)).
		Limit(limit).
		All(ctx)
}

func (r *taskSchedulablesRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.client.TaskSchedulable.Delete().Where(taskschedulable.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskSchedulablesRepo) Get(ctx context.Context, taskID int64) (*ent.TaskSchedulable, error) {
	return r.client.TaskSchedulable.Query().Where(taskschedulable.ID(taskID)).Only(ctx)
}
