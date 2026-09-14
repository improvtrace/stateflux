package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskcompleted"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskCompletedsRepo 是 repository.TaskCompletedsRepository 的 ent 实现（§3.1）。
type taskCompletedsRepo struct {
	client *ent.Client
}

// NewTaskCompleteds 构造 task_completeds 仓储；事务内复用时传入 tx.Client()。
func NewTaskCompleteds(client *ent.Client) repository.TaskCompletedsRepository {
	return &taskCompletedsRepo{client: client}
}

func (r *taskCompletedsRepo) Create(ctx context.Context, tasks []*ent.TaskCompleted) error {
	if len(tasks) == 0 {
		return nil
	}
	builders := make([]*ent.TaskCompletedCreate, 0, len(tasks))
	for _, t := range tasks {
		builders = append(builders, r.client.TaskCompleted.Create().
			SetID(t.ID).
			SetType(t.Type).
			SetOperator(t.Operator).
			SetPriority(t.Priority).
			SetChannel(t.Channel).
			SetTimeoutMs(t.TimeoutMs).
			SetMaxAttempts(t.MaxAttempts).
			SetAttempts(t.Attempts).
			SetClaimedNode(t.ClaimedNode).
			SetError(t.Error).
			SetIdempotencyKey(t.IdempotencyKey).
			SetCallback(t.Callback).
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
			SetOutcome(t.Outcome).
			SetCompletedAt(t.CompletedAt))
	}
	_, err := r.client.TaskCompleted.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskCompletedsRepo) CountByBizBatchID(ctx context.Context, bizBatchID string) (int, error) {
	return r.client.TaskCompleted.Query().Where(taskcompleted.BizBatchID(bizBatchID)).Count(ctx)
}

func (r *taskCompletedsRepo) ListDeadLetters(ctx context.Context, limit int) ([]*ent.TaskCompleted, error) {
	return r.client.TaskCompleted.Query().
		Where(taskcompleted.OutcomeEQ(int8(schema.OutcomeDead))).
		Order(ent.Desc(taskcompleted.FieldCompletedAt)).
		Limit(limit).
		All(ctx)
}

func (r *taskCompletedsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskCompleted, error) {
	return r.client.TaskCompleted.Query().Where(taskcompleted.ID(taskID)).Only(ctx)
}
