package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpending"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskPendingsRepo 是 repository.TaskPendingsRepository 的 ent 实现（§3.1）。
type taskPendingsRepo struct {
	data *Data
}

// NewTaskPendings 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskPendings(data *Data) repository.TaskPendingsRepository {
	return &taskPendingsRepo{data: data}
}

func (r *taskPendingsRepo) Create(ctx context.Context, tasks []*ent.TaskPending) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskPendingCreate, 0, len(tasks))
	for _, t := range tasks {
		builders = append(builders, r.data.db.TaskPending.Create().
			SetID(t.ID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(t.Priority).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
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
	_, err := r.data.db.TaskPending.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskPendingsRepo) ListForPromotion(ctx context.Context, limit int) ([]*ent.TaskPending, error) {
	return r.data.db.TaskPending.Query().
		Order(ent.Desc(taskpending.FieldPriority)).
		Limit(limit).
		All(ctx)
}

func (r *taskPendingsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskPending.Delete().Where(taskpending.IDIn(ids...)).Exec(ctx)
	return err
}

func (r *taskPendingsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskPending, error) {
	return r.data.db.TaskPending.Query().Where(taskpending.ID(taskID)).Only(ctx)
}
