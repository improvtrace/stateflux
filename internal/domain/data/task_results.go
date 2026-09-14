package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskresult"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskResultsRepo 是 repository.TaskResultsRepository 的 ent 实现（§3.1/§5.5）。
type taskResultsRepo struct {
	data *Data
}

// NewTaskResults 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskResults(data *Data) repository.TaskResultsRepository {
	return &taskResultsRepo{data: data}
}

func (r *taskResultsRepo) Create(ctx context.Context, results []*ent.TaskResult) ([]bool, error) {
	created := make([]bool, len(results))
	for i, result := range results {
		err := r.data.db.TaskResult.Create().
			SetID(result.ID).
			SetOutcome(result.Outcome).
			SetAttempt(result.Attempt).
			SetPayload(result.Payload).
			SetResult(result.Result).
			SetError(result.Error).
			SetCompletedAt(result.CompletedAt).
			Exec(ctx)
		switch {
		case err == nil:
			created[i] = true
		case ent.IsConstraintError(err):
			// 墓碑已存在：重复 ResultEvent 跳过（§5.5）。
			created[i] = false
		default:
			return nil, err
		}
	}
	return created, nil
}

func (r *taskResultsRepo) Get(ctx context.Context, taskID int64) (*ent.TaskResult, error) {
	return r.data.db.TaskResult.Query().Where(taskresult.ID(taskID)).Only(ctx)
}
