package data

import (
	"context"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskidentity"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskIdentitiesRepo 是 repository.TaskIdentitiesRepository 的 ent 实现（§3.1/§5.1）。
type taskIdentitiesRepo struct {
	data *Data
}

// NewTaskIdentities 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskIdentities(data *Data) repository.TaskIdentitiesRepository {
	return &taskIdentitiesRepo{data: data}
}

func (r *taskIdentitiesRepo) Put(ctx context.Context, identities []*ent.TaskIdentity) ([]int64, []bool, error) {
	ids := make([]int64, len(identities))
	created := make([]bool, len(identities))
	for i, identity := range identities {
		err := r.data.db.TaskIdentity.Create().
			SetID(identity.ID).
			SetIdempotencyKey(identity.IdempotencyKey).
			Exec(ctx)
		switch {
		case err == nil:
			ids[i], created[i] = identity.ID, true
		case ent.IsConstraintError(err):
			// dedupe 命中：返回账本中已有的原 task_id（§5.1）。
			row, gerr := r.GetByIdempotencyKey(ctx, identity.IdempotencyKey)
			if gerr != nil {
				return nil, nil, gerr
			}
			ids[i], created[i] = row.ID, false
		default:
			return nil, nil, err
		}
	}
	return ids, created, nil
}

func (r *taskIdentitiesRepo) GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*ent.TaskIdentity, error) {
	return r.data.db.TaskIdentity.Query().Where(taskidentity.IdempotencyKey(idempotencyKey)).Only(ctx)
}

func (r *taskIdentitiesRepo) DeleteCreatedBefore(ctx context.Context, cutoff time.Time) (int, error) {
	return r.data.db.TaskIdentity.Delete().Where(taskidentity.CreatedAtLT(cutoff)).Exec(ctx)
}
