package data

import (
	"context"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskresult"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskResultsRepo 是 repository.TaskResultsRepository 的 ent 实现（§3.1/§5.5）。
type taskResultsRepo struct {
	client *ent.Client
}

// NewTaskResults 构造 task_results 仓储；事务内复用时传入 tx.Client()。
func NewTaskResults(client *ent.Client) repository.TaskResultsRepository {
	return &taskResultsRepo{client: client}
}

func (r *taskResultsRepo) Create(ctx context.Context, results []*ent.TaskResult) ([]bool, error) {
	created := make([]bool, len(results))
	for i, result := range results {
		err := r.client.TaskResult.Create().
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
	return r.client.TaskResult.Query().Where(taskresult.ID(taskID)).Only(ctx)
}
