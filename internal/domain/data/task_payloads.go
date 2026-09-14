package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpayload"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskPayloadsRepo 是 repository.TaskPayloadsRepository 的 ent 实现（§3.1/§5.5）。
type taskPayloadsRepo struct {
	data *Data
}

// NewTaskPayloads 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskPayloads(data *Data) repository.TaskPayloadsRepository {
	return &taskPayloadsRepo{data: data}
}

func (r *taskPayloadsRepo) Create(ctx context.Context, payloads []*ent.TaskPayload) error {
	if len(payloads) == 0 {
		return nil
	}
	builders := make([]*ent.TaskPayloadCreate, 0, len(payloads))
	for _, p := range payloads {
		builders = append(builders, r.data.db.TaskPayload.Create().
			SetID(p.ID).
			SetPayload(p.Payload).
			SetCreatedAt(p.CreatedAt))
	}
	_, err := r.data.db.TaskPayload.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskPayloadsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskPayload, error) {
	return r.data.db.TaskPayload.Query().Where(taskpayload.ID(taskID)).Only(ctx)
}

func (r *taskPayloadsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskPayload.Delete().Where(taskpayload.IDIn(ids...)).Exec(ctx)
	return err
}
