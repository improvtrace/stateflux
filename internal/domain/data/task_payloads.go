package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpayload"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskPayloadsRepo 是 repository.TaskPayloadsRepository 的 ent 实现（§3.1/§5.5）。
type taskPayloadsRepo struct {
	client *ent.Client
}

// NewTaskPayloads 构造 task_payloads 仓储；事务内复用时传入 tx.Client()。
func NewTaskPayloads(client *ent.Client) repository.TaskPayloadsRepository {
	return &taskPayloadsRepo{client: client}
}

func (r *taskPayloadsRepo) Create(ctx context.Context, payloads []*ent.TaskPayload) error {
	if len(payloads) == 0 {
		return nil
	}
	builders := make([]*ent.TaskPayloadCreate, 0, len(payloads))
	for _, p := range payloads {
		builders = append(builders, r.client.TaskPayload.Create().
			SetID(p.ID).
			SetPayload(p.Payload).
			SetCreatedAt(p.CreatedAt))
	}
	_, err := r.client.TaskPayload.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskPayloadsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskPayload, error) {
	return r.client.TaskPayload.Query().Where(taskpayload.ID(taskID)).Only(ctx)
}

func (r *taskPayloadsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.client.TaskPayload.Delete().Where(taskpayload.IDIn(ids...)).Exec(ctx)
	return err
}
