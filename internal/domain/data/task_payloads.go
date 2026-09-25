package data

import (
	"context"
	"encoding/json"

	"encoding/json/jsontext"

	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpayload"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// taskPayloadsRepo 是 repository.TaskPayloadsRepository 的 ent 实现（§3.1/§5.5）：
// 仓储实体与 ent 实体的转换集中在本文件（§8）。
type taskPayloadsRepo struct {
	data *Data
}

// NewTaskPayloads 从 Data 构造仓储；事务内的仓储请经 d.WithTx(tx) 构造（§5.2/§5.5）。
func NewTaskPayloads(data *Data) repository.TaskPayloadsRepository {
	return &taskPayloadsRepo{data: data}
}

func (r *taskPayloadsRepo) Create(ctx context.Context, payloads []repository.Payload) error {
	if len(payloads) == 0 {
		return nil
	}
	builders := make([]*ent.TaskPayloadCreate, 0, len(payloads))
	for _, p := range payloads {
		payload := jsontext.Value(p.Payload)
		if len(payload) == 0 {
			payload = jsontext.Value("null")
		}
		builders = append(builders, r.data.db.TaskPayload.Create().
			SetID(p.TaskID).
			SetPayload(payload).
			SetCreatedAt(p.CreatedAt))
	}
	_, err := r.data.db.TaskPayload.CreateBulk(builders...).Save(ctx)
	return err
}

func (r *taskPayloadsRepo) Get(ctx context.Context, taskID int64) (*repository.Payload, error) {
	row, err := r.data.db.TaskPayload.Query().Where(taskpayload.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	p := payloadFromEnt(row)
	return &p, nil
}

func (r *taskPayloadsRepo) DeleteByIDs(ctx context.Context, ids []int64) error {
	_, err := r.data.db.TaskPayload.Delete().Where(taskpayload.IDIn(ids...)).Exec(ctx)
	return err
}

// payloadFromEnt 把 ent 实体投影为仓储实体（§5.5）。
func payloadFromEnt(row *ent.TaskPayload) repository.Payload {
	return repository.Payload{
		TaskID:    row.ID,
		Payload:   json.RawMessage(row.Payload),
		CreatedAt: row.CreatedAt,
	}
}
