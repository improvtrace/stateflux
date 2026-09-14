package data

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"time"

	entsql "entgo.io/ent/dialect/sql"

	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskidentity"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpayload"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpending"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskprocessing"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskresult"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

var (
	// errStaleAttempt 表示 processing 行不存在，或 attempts 与结果携带的 attempt 不符：陈旧结果
	// 不得产生任何副作用（§5.5/§6.2）。
	errStaleAttempt = errors.New("data: stale attempt or missing processing row")
	// errDuplicateResult 表示 task_results 墓碑已存在：重复归集，回滚整个终态事务（§5.5）。
	errDuplicateResult = errors.New("data: duplicate task result")
)

// upsertIdentitySQL 是身份账本的幂等写入（§5.1）：键冲突时不报错也不更新，由调用方回读原
// task_id。DO NOTHING 不会让 PG 事务进入 aborted 状态，故可在同一事务内继续回读。
const upsertIdentitySQL = `INSERT INTO task_identities (task_id, idempotency_key, created_at)
VALUES ($1, $2, $3)
ON CONFLICT (idempotency_key) DO NOTHING`

// claimSQL 是认领挪行的单条原生 SQL（§3.1/§5.2）：FOR UPDATE SKIP LOCKED 选候选，删除
// schedulable 源行并以 attempts + 1、updated_at = now() 插入 processing。并发调度节点取到互不
// 重叠的候选；attempt 只在此处递增，构成执行 fence。列顺序与 claimScan 一一对应。
const claimSQL = `WITH picked AS (
    SELECT id
      FROM task_schedulables
     ORDER BY priority DESC, id
     LIMIT $1
       FOR UPDATE SKIP LOCKED
),
moved AS (
    DELETE FROM task_schedulables AS s
     USING picked AS p
     WHERE s.id = p.id
    RETURNING s.id, s.type, s.operator, s.priority, s.channel, s.timeout_ms,
              s.max_attempts, s.attempts, s.claimed_node, s.idempotency_key,
              s.callback, s.parent_task_id, s.vpc, s.node, s.label, s.hash_bucket,
              s.biz_race_labels, s.biz_race_entry, s.biz_group, s.biz_batch_id,
              s.created_at, s.updated_at
)
INSERT INTO task_processings (
    id, type, operator, priority, channel, timeout_ms, max_attempts, attempts,
    claimed_node, idempotency_key, callback, parent_task_id, vpc, node, label,
    hash_bucket, biz_race_labels, biz_race_entry, biz_group, biz_batch_id,
    created_at, updated_at
)
SELECT id, type, operator, priority, channel, timeout_ms, max_attempts, attempts + 1,
       claimed_node, idempotency_key, callback, parent_task_id, vpc, node, label,
       hash_bucket, biz_race_labels, biz_race_entry, biz_group, biz_batch_id,
       created_at, now()
  FROM moved
RETURNING id, type, operator, priority, channel, timeout_ms, max_attempts, attempts,
          claimed_node, idempotency_key, callback, parent_task_id, vpc, node, label,
          hash_bucket, biz_race_labels, biz_race_entry, biz_group, biz_batch_id,
          created_at, updated_at`

// store 是 repository.Store 的 ent 实现（§3.1）：单表仓储由构造器注入，跨表方法自行开事务。
type store struct {
	data *Data

	// nextID 为终态事务内派生的回调任务生成 task_id（§5.6）；默认雪花，可注入替换。
	nextID func() int64

	pendings     repository.TaskPendingsRepository
	schedulables repository.TaskSchedulablesRepository
	processings  repository.TaskProcessingsRepository
	completeds   repository.TaskCompletedsRepository
	payloads     repository.TaskPayloadsRepository
	results      repository.TaskResultsRepository
	identities   repository.TaskIdentitiesRepository
}

// store 必须完整实现组合根契约。
var _ repository.Store = (*store)(nil)

// NewStore 从 Data 构造组合根 Store（§3.1/§5.2/§5.5）：回调派生任务使用默认雪花生成器。
func NewStore(data *Data) repository.Store {
	return NewStoreWithIDGenerator(data, domain.NewSnowflake("callback").Next)
}

// NewStoreWithIDGenerator 允许注入派生任务 ID 生成器（装配期可复用节点雪花，保证全局唯一）。
func NewStoreWithIDGenerator(data *Data, next func() int64) repository.Store {
	if next == nil {
		next = domain.NewSnowflake("callback").Next
	}
	return &store{
		data:         data,
		nextID:       next,
		pendings:     NewTaskPendings(data),
		schedulables: NewTaskSchedulables(data),
		processings:  NewTaskProcessings(data),
		completeds:   NewTaskCompleteds(data),
		payloads:     NewTaskPayloads(data),
		results:      NewTaskResults(data),
		identities:   NewTaskIdentities(data),
	}
}

// WithTx 透传 Data 的事务封装（§5.2/§5.5）。
func (s *store) WithTx(ctx context.Context, fn func(context.Context) error) error {
	return s.data.WithTx(ctx, fn)
}

func (s *store) TaskPendings() repository.TaskPendingsRepository         { return s.pendings }
func (s *store) TaskSchedulables() repository.TaskSchedulablesRepository { return s.schedulables }
func (s *store) TaskProcessings() repository.TaskProcessingsRepository   { return s.processings }
func (s *store) TaskCompleteds() repository.TaskCompletedsRepository     { return s.completeds }
func (s *store) TaskPayloads() repository.TaskPayloadsRepository         { return s.payloads }
func (s *store) TaskResults() repository.TaskResultsRepository           { return s.results }
func (s *store) TaskIdentities() repository.TaskIdentitiesRepository     { return s.identities }

// txClient 取事务绑定的 ent client；无事务时回落到全局 client（只读路径）。跨表方法都在
// WithTx 回调内调用它，保证读写在同一个 PG 事务上。
func (s *store) txClient(ctx context.Context) *ent.Client {
	if tx, ok := ctx.Value(tranCtxKey{}).(*ent.Tx); ok {
		return tx.Client()
	}
	return s.data.db
}

// Enqueue 在一个 PG 事务内写 identity、payload 与 pending（§5.1）：身份账本裁决幂等，dedupe
// 命中时返回原 task_id 且不重复写 payload/pending。
func (s *store) Enqueue(ctx context.Context, req repository.EnqueueRequest) (repository.EnqueueResult, error) {
	createdAt := req.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	updatedAt := req.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	var out repository.EnqueueResult
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		c := s.txClient(ctx)

		taskID, created := req.TaskID, true
		if req.IdempotencyKey != "" {
			res, err := c.ExecContext(ctx, upsertIdentitySQL, req.TaskID, req.IdempotencyKey, createdAt)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if created = n == 1; !created {
				row, err := c.TaskIdentity.Query().
					Where(taskidentity.IdempotencyKey(req.IdempotencyKey)).
					Only(ctx)
				if err != nil {
					return err
				}
				taskID = row.ID
			}
		}
		out = repository.EnqueueResult{TaskID: taskID, Created: created}
		if !created {
			// dedupe 命中：payload 与 pending 已在首次创建时写入，不重复写（§5.1）。
			return nil
		}

		// payload 非空约束：缺省写入 JSON null，避免把零值写成 NOT NULL 违约。
		payload := jsontext.Value(req.Payload)
		if len(payload) == 0 {
			payload = jsontext.Value("null")
		}
		if err := c.TaskPayload.Create().
			SetID(taskID).
			SetPayload(payload).
			SetCreatedAt(createdAt).
			Exec(ctx); err != nil {
			return err
		}

		create := c.TaskPending.Create().
			SetID(taskID).
			SetType(req.Type).
			SetOperator(req.Operator).
			SetPriority(int8(req.Priority)).
			SetChannel(req.Channel).
			SetTimeoutMs(req.TimeoutMs).
			SetMaxAttempts(req.MaxAttempts).
			SetIdempotencyKey(req.IdempotencyKey).
			SetParentTaskID(req.ParentTaskID).
			SetVpc(req.Vpc).
			SetNode(req.Node).
			SetLabel(req.Label).
			SetHashBucket(req.HashBucket).
			SetBizRaceLabels(req.BizRaceLabels).
			SetBizRaceEntry(req.BizRaceEntry).
			SetBizGroup(req.BizGroup).
			SetBizBatchID(req.BizBatchID).
			SetCreatedAt(createdAt).
			SetUpdatedAt(updatedAt)
		if len(req.Callback) > 0 {
			create = create.SetCallback(jsontext.Value(req.Callback))
		}
		return create.Exec(ctx)
	})
	if err != nil {
		return repository.EnqueueResult{}, err
	}
	return out, nil
}

// Promote 批量 pending → schedulable 并删除源行（§5.2）：候选按 priority DESC，FOR UPDATE
// SKIP LOCKED 保证并发 Promoter 不重复挪行。attempts/claimed_node 由 claim 产生，此处保持零值；
// hash_bucket 等公共段列原样保留。
func (s *store) Promote(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	var promoted int
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		c := s.txClient(ctx)
		rows, err := c.TaskPending.Query().
			Order(ent.Desc(taskpending.FieldPriority)).
			Limit(limit).
			ForUpdate(entsql.WithLockAction(entsql.SkipLocked)).
			All(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		builders := make([]*ent.TaskSchedulableCreate, 0, len(rows))
		ids := make([]int64, 0, len(rows))
		for _, p := range rows {
			create := c.TaskSchedulable.Create().
				SetID(p.ID).
				SetType(p.Type).
				SetOperator(p.Operator).
				SetPriority(p.Priority).
				SetChannel(p.Channel).
				SetTimeoutMs(p.TimeoutMs).
				SetMaxAttempts(p.MaxAttempts).
				SetAttempts(0).
				SetClaimedNode("").
				SetIdempotencyKey(p.IdempotencyKey).
				SetParentTaskID(p.ParentTaskID).
				SetVpc(p.Vpc).
				SetNode(p.Node).
				SetLabel(p.Label).
				SetHashBucket(p.HashBucket).
				SetBizRaceLabels(p.BizRaceLabels).
				SetBizRaceEntry(p.BizRaceEntry).
				SetBizGroup(p.BizGroup).
				SetBizBatchID(p.BizBatchID).
				SetCreatedAt(p.CreatedAt).
				SetUpdatedAt(p.UpdatedAt)
			if len(p.Callback) > 0 {
				create = create.SetCallback(p.Callback)
			}
			builders = append(builders, create)
			ids = append(ids, p.ID)
		}
		if _, err := c.TaskSchedulable.CreateBulk(builders...).Save(ctx); err != nil {
			return err
		}
		if _, err := c.TaskPending.Delete().Where(taskpending.IDIn(ids...)).Exec(ctx); err != nil {
			return err
		}
		promoted = len(rows)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return promoted, nil
}

// Claim 以单条原生 SQL 把 schedulable → processing（§3.1/§5.2）：FOR UPDATE SKIP LOCKED 选
// 候选、删除源行、attempts + 1 后插入 processing，全在一个事务内。attempt 是执行 fence，只在
// 认领时递增。
//
// 注意：Store.Claim 契约不接收节点参数，故这里不写入本次认领节点，claimed_node 沿用源行上的
// 上次认领值（首次认领为空串），避免把错误的节点写进诊断列；节点身份接入见 §14。
func (s *store) Claim(ctx context.Context, limit int) ([]repository.Claimed, error) {
	if limit <= 0 {
		return nil, nil
	}
	var claimed []repository.Claimed
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		rows, err := s.txClient(ctx).QueryContext(ctx, claimSQL, limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				row         repository.Claimed
				priority    int64
				timeoutMs   int64
				maxAttempts int64
				hashBucket  int64
				callback    []byte
				raceLabels  []byte
			)
			if err := rows.Scan(
				&row.TaskID,
				&row.Type,
				&row.Operator,
				&priority,
				&row.Channel,
				&timeoutMs,
				&maxAttempts,
				&row.Attempt,
				&row.ClaimedNode,
				&row.IdempotencyKey,
				&callback,
				&row.ParentTaskID,
				&row.Vpc,
				&row.Node,
				&row.Label,
				&hashBucket,
				&raceLabels,
				&row.BizRaceEntry,
				&row.BizGroup,
				&row.BizBatchID,
				&row.CreatedAt,
				&row.UpdatedAt,
			); err != nil {
				return err
			}
			row.Priority = schema.Priority(priority)
			row.TimeoutMs = timeoutMs
			row.MaxAttempts = int32(maxAttempts)
			row.HashBucket = int16(hashBucket)
			row.Callback = json.RawMessage(callback)
			if len(raceLabels) > 0 {
				if err := json.Unmarshal(raceLabels, &row.BizRaceLabels); err != nil {
					return fmt.Errorf("data: decode biz_race_labels of task %d: %w", row.TaskID, err)
				}
			}
			claimed = append(claimed, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// Complete 校验 attempt fence 后执行终态搬移（§5.5）：写 completed、写 task_results 墓碑、
// 删 payload、删 processing，全部在一个事务内。attempt 不匹配或行不存在返回 false 且无副作用；
// 墓碑主键冲突即重复归集，回滚并返回 false。
func (s *store) Complete(ctx context.Context, req repository.CompleteRequest) (bool, error) {
	completedAt := req.CompletedAt
	if completedAt.IsZero() {
		completedAt = time.Now()
	}
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		return terminalMove(ctx, s.txClient(ctx), s.nextID, req.TaskID, req.Attempt, req.Outcome, req.Result, req.Error, completedAt, true)
	})
	return terminalOutcome(err)
}

// DeadLetter 是 R3 死信终态（§6.2）：不做 attempt fence，直接以 outcome=dead 走终态搬移
// （completed + task_results、删 payload、删 processing）。行不存在或墓碑已存在返回 false。
func (s *store) DeadLetter(ctx context.Context, taskID int64) (bool, error) {
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		return terminalMove(ctx, s.txClient(ctx), s.nextID, taskID, 0, schema.OutcomeDead, nil, "", time.Now(), false)
	})
	return terminalOutcome(err)
}

// terminalOutcome 把终态事务的哨兵错误翻译为「拒绝且无副作用」的 false。
func terminalOutcome(err error) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errStaleAttempt), errors.Is(err, errDuplicateResult):
		return false, nil
	default:
		return false, err
	}
}

// terminalMove 在调用方事务内完成一次终态搬移（§5.5）：读 processing（可选 attempt fence），
// 合并 payload 入 task_results，写 completed 与 results 墓碑，删除分离的 payload 与 processing。
func terminalMove(ctx context.Context, c *ent.Client, nextID func() int64, taskID, attempt int64, outcome schema.Outcome, result json.RawMessage, errMsg string, completedAt time.Time, checkAttempt bool) error {
	proc, err := c.TaskProcessing.Query().Where(taskprocessing.ID(taskID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return errStaleAttempt
		}
		return err
	}
	if checkAttempt && proc.Attempts != attempt {
		// attempt fence：陈旧 ResultEvent 不得产生任何副作用（§5.5/§6.2）。
		return errStaleAttempt
	}

	var payload []byte
	switch p, qerr := c.TaskPayload.Query().Where(taskpayload.ID(taskID)).Only(ctx); {
	case qerr == nil:
		payload = p.Payload
	case ent.IsNotFound(qerr):
		// payload 可能已被清理：终态仍写墓碑（§5.5）。
	default:
		return qerr
	}

	completed := c.TaskCompleted.Create().
		SetID(proc.ID).
		SetType(proc.Type).
		SetOperator(proc.Operator).
		SetPriority(proc.Priority).
		SetChannel(proc.Channel).
		SetTimeoutMs(proc.TimeoutMs).
		SetMaxAttempts(proc.MaxAttempts).
		SetAttempts(proc.Attempts).
		SetClaimedNode(proc.ClaimedNode).
		SetError(errMsg).
		SetIdempotencyKey(proc.IdempotencyKey).
		SetParentTaskID(proc.ParentTaskID).
		SetVpc(proc.Vpc).
		SetNode(proc.Node).
		SetLabel(proc.Label).
		SetBizRaceLabels(proc.BizRaceLabels).
		SetBizRaceEntry(proc.BizRaceEntry).
		SetBizGroup(proc.BizGroup).
		SetBizBatchID(proc.BizBatchID).
		SetCreatedAt(proc.CreatedAt).
		SetUpdatedAt(proc.UpdatedAt).
		SetOutcome(int8(outcome)).
		SetCompletedAt(completedAt)
	if len(proc.Callback) > 0 {
		completed = completed.SetCallback(proc.Callback)
	}
	if err := completed.Exec(ctx); err != nil {
		return err
	}

	rec := c.TaskResult.Create().
		SetID(taskID).
		SetOutcome(int8(outcome)).
		SetAttempt(proc.Attempts).
		SetError(errMsg).
		SetCompletedAt(completedAt)
	if len(payload) > 0 {
		rec = rec.SetPayload(jsontext.Value(payload))
	}
	if len(result) > 0 {
		rec = rec.SetResult(jsontext.Value(result))
	}
	if err := rec.Exec(ctx); err != nil {
		if ent.IsConstraintError(err) {
			// task_id 主键冲突：墓碑已存在，重复归集（§5.5）。
			return errDuplicateResult
		}
		return err
	}

	if _, err := c.TaskPayload.Delete().Where(taskpayload.ID(taskID)).Exec(ctx); err != nil {
		return err
	}
	if _, err := c.TaskProcessing.Delete().Where(taskprocessing.ID(taskID)).Exec(ctx); err != nil {
		return err
	}
	// 终态事务内派生回调（§5.6）：同事务写入保证「终态与派生」原子可见。
	return deriveCallback(ctx, c, nextID, proc, outcome, completedAt)
}

// ResetExpired 是 R1 对账（§6.2）：updated_at 早于 deadline 的 processing 行移回 schedulable
// 并删除源行，attempts/claimed_node 原样保留（重置不递增 attempt），重置后立即重新可认领。
func (s *store) ResetExpired(ctx context.Context, deadline time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	var reset int
	err := s.data.WithTx(ctx, func(ctx context.Context) error {
		c := s.txClient(ctx)
		rows, err := c.TaskProcessing.Query().
			Where(taskprocessing.UpdatedAtLT(deadline)).
			Order(ent.Asc(taskprocessing.FieldUpdatedAt)).
			Limit(limit).
			ForUpdate(entsql.WithLockAction(entsql.SkipLocked)).
			All(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		builders := make([]*ent.TaskSchedulableCreate, 0, len(rows))
		ids := make([]int64, 0, len(rows))
		for _, p := range rows {
			create := c.TaskSchedulable.Create().
				SetID(p.ID).
				SetType(p.Type).
				SetOperator(p.Operator).
				SetPriority(p.Priority).
				SetChannel(p.Channel).
				SetTimeoutMs(p.TimeoutMs).
				SetMaxAttempts(p.MaxAttempts).
				SetAttempts(p.Attempts).
				SetClaimedNode(p.ClaimedNode).
				SetIdempotencyKey(p.IdempotencyKey).
				SetParentTaskID(p.ParentTaskID).
				SetVpc(p.Vpc).
				SetNode(p.Node).
				SetLabel(p.Label).
				SetHashBucket(p.HashBucket).
				SetBizRaceLabels(p.BizRaceLabels).
				SetBizRaceEntry(p.BizRaceEntry).
				SetBizGroup(p.BizGroup).
				SetBizBatchID(p.BizBatchID).
				SetCreatedAt(p.CreatedAt).
				SetUpdatedAt(p.UpdatedAt)
			if len(p.Callback) > 0 {
				create = create.SetCallback(p.Callback)
			}
			builders = append(builders, create)
			ids = append(ids, p.ID)
		}
		if _, err := c.TaskSchedulable.CreateBulk(builders...).Save(ctx); err != nil {
			return err
		}
		if _, err := c.TaskProcessing.Delete().Where(taskprocessing.IDIn(ids...)).Exec(ctx); err != nil {
			return err
		}
		reset = len(rows)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reset, nil
}

// GetProcessing 按 task_id 读取在途行（不存在返回 ent.NotFoundError）。
func (s *store) GetProcessing(ctx context.Context, taskID int64) (*repository.Processing, error) {
	p, err := s.data.db.TaskProcessing.Query().Where(taskprocessing.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	row := processingFromEnt(p)
	return &row, nil
}

// GetResult 按 task_id 读取终态结果（不存在返回 ent.NotFoundError）。
func (s *store) GetResult(ctx context.Context, taskID int64) (*repository.Result, error) {
	r, err := s.data.db.TaskResult.Query().Where(taskresult.ID(taskID)).Only(ctx)
	if err != nil {
		return nil, err
	}
	return &repository.Result{
		TaskID:      r.ID,
		Outcome:     schema.Outcome(r.Outcome),
		Attempt:     r.Attempt,
		Payload:     json.RawMessage(r.Payload),
		Result:      json.RawMessage(r.Result),
		Error:       r.Error,
		CompletedAt: r.CompletedAt,
	}, nil
}

// CountPending 返回待晋升行数。
func (s *store) CountPending(ctx context.Context) (int, error) {
	return s.data.db.TaskPending.Query().Count(ctx)
}

// CountSchedulable 返回就绪行数。
func (s *store) CountSchedulable(ctx context.Context) (int, error) {
	return s.data.db.TaskSchedulable.Query().Count(ctx)
}

// CountProcessing 返回在途行数。
func (s *store) CountProcessing(ctx context.Context) (int, error) {
	return s.data.db.TaskProcessing.Query().Count(ctx)
}

// processingFromEnt 把 ent 实体投影为组合根只读结构（§5.2）。
func processingFromEnt(p *ent.TaskProcessing) repository.Processing {
	return repository.Processing{
		TaskID:         p.ID,
		Type:           p.Type,
		Operator:       p.Operator,
		Priority:       schema.Priority(p.Priority),
		Channel:        p.Channel,
		TimeoutMs:      p.TimeoutMs,
		MaxAttempts:    p.MaxAttempts,
		Attempt:        p.Attempts,
		ClaimedNode:    p.ClaimedNode,
		IdempotencyKey: p.IdempotencyKey,
		Callback:       json.RawMessage(p.Callback),
		ParentTaskID:   p.ParentTaskID,
		Vpc:            p.Vpc,
		Node:           p.Node,
		Label:          p.Label,
		HashBucket:     p.HashBucket,
		BizRaceLabels:  p.BizRaceLabels,
		BizRaceEntry:   p.BizRaceEntry,
		BizGroup:       p.BizGroup,
		BizBatchID:     p.BizBatchID,
		CreatedAt:      p.CreatedAt,
		UpdatedAt:      p.UpdatedAt,
	}
}
