package data

import (
	"context"
	"encoding/json"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskresult"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// taskResultsRepo 是 repository.TaskResultsRepository 的 ent 实现（§3.1/§5.5）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskResultsRepo struct {
	data *Data
}

// NewTaskResults 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskResults(data *Data) repository.TaskResultsRepository {
	return &taskResultsRepo{data: data}
}

func (r *taskResultsRepo) Create(ctx context.Context, results []repository.Result) ([]bool, error) {
	created := make([]bool, len(results))
	for i, result := range results {
		create := r.data.db.TaskResult.Create().
			SetID(result.TaskID).
			SetOutcome(int8(result.Outcome)).
			SetAttempt(result.Attempt).
			SetError(result.Error).
			SetCompletedAt(result.CompletedAt)
		if len(result.Payload) > 0 {
			create = create.SetPayload(jsontext.Value(result.Payload))
		}
		if len(result.Result) > 0 {
			create = create.SetResult(jsontext.Value(result.Result))
		}
		err := create.Exec(ctx)
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

func (r *taskResultsRepo) Get(ctx context.Context, taskID int64) (*repository.Result, error) {
	row, err := r.data.db.TaskResult.Query().Where(taskresult.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	res := resultFromEnt(row)
	return &res, nil
}

// resultFromEnt 把 ent 实体投影为仓储实体（§5.5）。
func resultFromEnt(row *ent.TaskResult) repository.Result {
	return repository.Result{
		TaskID:      row.ID,
		Outcome:     schema.Outcome(row.Outcome),
		Attempt:     row.Attempt,
		Payload:     json.RawMessage(row.Payload),
		Result:      json.RawMessage(row.Result),
		Error:       row.Error,
		CompletedAt: row.CompletedAt,
	}
}
