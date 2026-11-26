package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"entgo.io/ent/dialect/sql"

	"github.com/improvtrace/stateflux/states/ent"
	"github.com/improvtrace/stateflux/states/ent/taskpersistent"
)

// ErrTaskNotFound 表示任务行不存在（Get）。
var ErrTaskNotFound = errors.New("states: task not found")

// 任务持久化表 task_persistents 的调度侧状态机（两阶段发布）。
//
// 任务在 dispatch 之前先落表（pending），随后由发布通道维护各阶段状态：
// 认领（pending→dispatching）与发布确认（dispatching→dispatched）分离，
// **发布成功才置 dispatched**——发布失败或中途崩溃的行停留在 dispatching，
// 由对账回收重派。服务重启/停止后，非终态行（未执行完）会在启动时被
// 重新 dispatch 到任务队列。由此产生至少一次投递语义：发布成功但未及
// MarkDispatched 即崩溃的行会被重新发布，消费端必须按 IdempotencyKey 幂等。
//
//	pending ──claim──▶ dispatching ──publish ok──▶ dispatched ──finish──▶ terminal
//	   ▲                   │                             │
//	   └──requeue（发布失败/对账重派）◀──────────────────┘
type TaskPhase int8

const (
	TaskPhasePending     TaskPhase = iota // 已落表，待发布
	TaskPhaseDispatching                  // 已认领（两阶段第一步），待发布确认
	TaskPhaseDispatched                   // 已发布到队列，在途
	TaskPhaseTerminal                     // 终态，Outcome 字段生效
)

// String 返回可读名，用于日志与观测标签。
func (p TaskPhase) String() string {
	switch p {
	case TaskPhasePending:
		return "pending"
	case TaskPhaseDispatching:
		return "dispatching"
	case TaskPhaseDispatched:
		return "dispatched"
	case TaskPhaseTerminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// TaskOutcome 是终态类别，仅 TaskPhaseTerminal 时有效。零值 Unset 不是合法终态，
// 终态写入必须显式给出。
type TaskOutcome int8

const (
	TaskOutcomeUnset     TaskOutcome = iota // 缺省零值，不是合法终态
	TaskOutcomeSucceeded                    // 执行成功
	TaskOutcomeFailed                       // 执行失败且不再重试
	TaskOutcomeDead                         // 死信：重派次数用尽（保留枚举，计数由执行框架实施）
)

// String 返回可读名，用于日志与观测标签。
func (o TaskOutcome) String() string {
	switch o {
	case TaskOutcomeSucceeded:
		return "succeeded"
	case TaskOutcomeFailed:
		return "failed"
	case TaskOutcomeDead:
		return "dead"
	default:
		return "unset"
	}
}

// TaskPersistent 是 task_persistents 一行的领域映射。本表是调度事实源：
// Redis 队列无持久化，崩溃后队列内容可全丢，恢复由 phase + updated_at 对账
// 完成。执行内重试由异步任务框架承担（MaxAttempts 仅记录预算），表不累计
// 发布轮数，重派次数上限交由执行侧约束。
type TaskPersistent struct {
	Id             int64           // 任务 ID（雪花，客户端生成，非自增）
	Resource       string          // 任务资源名：决定 handler 与路由（路由 header AttributeResource）
	Operator       string          // 任务子类型（type 的细化），与 type 共同决定 handler
	Phase          TaskPhase       // 调度侧阶段，见 TaskPhase
	Outcome        TaskOutcome     // 终态类别，仅 Phase=Terminal 时有效
	ErrorSummary   string          // 终态错误摘要（成功为空）
	TimeoutMs      int64           // 单次执行预算（毫秒），对账超时阈值
	MaxAttempts    int32           // 最大重试次数（重试由异步任务框架实施）
	ClaimedNode    string          // 认领节点（ClaimPending 认领时写入）；随行注入 AttributeClaimedNode，选点时最优先匹配
	IdempotencyKey string          // 业务幂等键
	Payload        json.RawMessage // 业务参数（jsonb）
	Vpc            string          // 目标节点属性：网络域，空 = 不限；注入 AttributeVpcs
	Node           string          // 目标节点属性：用户指定的节点 ID，空 = 任意；注入 AttributeTargetNode，选点时优先匹配
	Label          string          // 目标节点属性：节点标签 key=value;…（同键可重复），空 = 不限；构造 Signature 时解析为 AttributeLabels
	HashBucket     int16           // 目标节点属性：哈希桶 0-255，0 = 不限；作选点散列键 AttributeHashBucket
	FinishedAt     *time.Time      // 终态时间，非终态为 nil
	CreatedAt      time.Time       // 创建时间（不可变）
	UpdatedAt      time.Time       // 最后更新时间，对账扫描依据
}

// TaskPersistentClaim 是认领条件。Resources 限定本次认领的任务资源集合，
// 空 = 全部资源（启动/对账恢复通道用）；Batch 限制认领行数；Node 写入
// claimed_node。
type TaskPersistentClaim struct {
	Resources []string
	Batch     int
	Node      string
}

// TaskPersistentRepo 是 task_persistents 的数据层接口。实现方必须保证：
//   - ClaimPending 用 FOR UPDATE SKIP LOCKED 认领，多节点并发安全，
//     原子完成 pending→dispatching、写 claimed_node
//   - Create 对 IdempotencyKey 冲突幂等（返回已有行，不报错）
//   - MarkDispatched / MarkTerminal 对阶段不匹配的行幂等（忽略）
type TaskPersistentRepo interface {
	// Create 落表（Phase 置 TaskPhasePending），dispatch 前调用。Id 由调用方
	// 雪花生成；IdempotencyKey 冲突时返回已存在的行。
	Create(ctx context.Context, m *TaskPersistent) (*TaskPersistent, error)

	// Get 按 ID 查询单行，不存在返回包裹 ErrTaskNotFound 的错误。
	Get(ctx context.Context, id int64) (*TaskPersistent, error)

	// ClaimPending 认领一批 pending 任务并置为 dispatching（两阶段第一步）。
	// 认领顺序按 CreatedAt ASC；发布成功后调 MarkDispatched 完成
	// dispatching→dispatched。
	ClaimPending(ctx context.Context, cond *TaskPersistentClaim) ([]*TaskPersistent, error)

	// Claim 按 ID 认领已落表的单行（pending→dispatching、写 claimed_node）。
	// 供持久化转发通道使用：只动指定行，不像 ClaimPending 那样按资源批量
	// 扫描。行不在 pending（已被认领/已发布/已终态）时返回 false，不报错。
	Claim(ctx context.Context, id int64, node string) (bool, error)

	// MarkDispatched 批量把 dispatching 行置为 dispatched（两阶段第二步，
	// 发布成功才调用）。非 dispatching 行忽略。
	MarkDispatched(ctx context.Context, ids []int64) error

	// MarkTerminal 写终态：Phase=Terminal、Outcome、ErrorSummary、FinishedAt。
	MarkTerminal(ctx context.Context, id int64, outcome TaskOutcome, errSummary string) error

	// Requeue 回 pending 等待重派（发布失败或对账回收），恢复通道会再次认领。
	Requeue(ctx context.Context, id int64) error

	// Inflight 返回 dispatching 或 dispatched 且 UpdatedAt 早于 olderThan 的行
	//（两阶段任一步卡住都算在途），按 UpdatedAt ASC 排序，limit 限制行数，
	// 供对账循环。
	Inflight(ctx context.Context, olderThan time.Time, limit int) ([]*TaskPersistent, error)
}

// 阶段迁移全部走 ent：认领在事务内 SELECT ... FOR UPDATE SKIP LOCKED 锁行后
// 批量 UPDATE，phase 取值统一引用 states 常量，避免查询条件与领域定义漂移。
var _ TaskPersistentRepo = &taskPersistentRepo{}

type taskPersistentRepo struct {
	client *ent.Client
}

// NewTaskPersistentRepo 构造任务表的 ent 实现。client 由调用方打开与关闭
// （states.Open），repo 内部不持有连接。
func NewTaskPersistentRepo(client *ent.Client) TaskPersistentRepo {
	return &taskPersistentRepo{client: client}
}

func (r *taskPersistentRepo) Create(ctx context.Context, m *TaskPersistent) (*TaskPersistent, error) {
	create := r.client.TaskPersistent.Create().
		SetID(m.Id).
		SetResource(m.Resource).
		SetOperator(m.Operator).
		// 落表恒为 pending，忽略调用方传入的阶段/终态
		SetPhase(int8(TaskPhasePending)).
		SetOutcome(int8(TaskOutcomeUnset)).
		SetErrorSummary(m.ErrorSummary).
		SetTimeoutMs(m.TimeoutMs).
		SetMaxAttempts(m.MaxAttempts).
		SetClaimedNode(m.ClaimedNode).
		SetIdempotencyKey(m.IdempotencyKey).
		SetVpc(m.Vpc).
		SetNode(m.Node).
		SetLabel(m.Label).
		SetHashBucket(m.HashBucket)
	if len(m.Payload) > 0 {
		create = create.SetPayload(m.Payload)
	}
	if m.FinishedAt != nil {
		create = create.SetFinishedAt(*m.FinishedAt)
	}
	if !m.CreatedAt.IsZero() {
		create = create.SetCreatedAt(m.CreatedAt)
	}
	if !m.UpdatedAt.IsZero() {
		create = create.SetUpdatedAt(m.UpdatedAt)
	}

	en, err := create.Save(ctx)
	if err != nil {
		// 幂等键冲突按接口约定不报错：返回已存在的行
		if ent.IsConstraintError(err) && m.IdempotencyKey != "" {
			slog.Debug("states: task row already exists for idempotency key",
				"idempotency_key", m.IdempotencyKey, "resource", m.Resource)
			return r.getByKey(ctx, m.IdempotencyKey)
		}
		return nil, err
	}
	return taskPersistent(en), nil
}

func (r *taskPersistentRepo) Get(ctx context.Context, id int64) (*TaskPersistent, error) {
	e, err := r.client.TaskPersistent.Get(ctx, id)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
		}
		return nil, err
	}
	return taskPersistent(e), nil
}

// ClaimPending 在事务内两步完成认领：先 SELECT ... FOR UPDATE SKIP LOCKED
// 锁定待认领行（多节点并发认领互不阻塞），再批量 UPDATE 原子完成
// pending→dispatching、写 claimed_node。认领结果在本事务内回读（行已锁定，
// 读到即认领后的状态），按接口契约要求以创建时间有序返回。
func (r *taskPersistentRepo) ClaimPending(ctx context.Context, cond *TaskPersistentClaim) ([]*TaskPersistent, error) {
	if cond.Batch <= 0 {
		return []*TaskPersistent{}, nil
	}
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	var out []*TaskPersistent
	if err := func() error {
		client := tx.Client()
		query := client.TaskPersistent.Query().
			Where(taskpersistent.PhaseEQ(int8(TaskPhasePending))).
			Order(ent.Asc(taskpersistent.FieldCreatedAt)).
			Limit(cond.Batch).
			ForUpdate(sql.WithLockAction(sql.SkipLocked))
		if len(cond.Resources) > 0 {
			query = query.Where(taskpersistent.ResourceIn(cond.Resources...))
		}
		rows, err := query.All(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			out = []*TaskPersistent{}
			return nil
		}
		ids := make([]int64, len(rows))
		for i, e := range rows {
			ids[i] = e.ID
		}
		if _, err := client.TaskPersistent.Update().
			Where(taskpersistent.IDIn(ids...)).
			SetPhase(int8(TaskPhaseDispatching)).
			SetClaimedNode(cond.Node).
			Save(ctx); err != nil {
			return err
		}
		claimed, err := client.TaskPersistent.Query().
			Where(taskpersistent.IDIn(ids...)).
			Order(ent.Asc(taskpersistent.FieldCreatedAt)).
			All(ctx)
		if err != nil {
			return err
		}
		out = make([]*TaskPersistent, 0, len(claimed))
		for _, e := range claimed {
			out = append(out, taskPersistent(e))
		}
		return nil
	}(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return out, tx.Commit()
}

// Claim 按 ID 认领单行：单条 UPDATE 原子完成 pending→dispatching 并写
// claimed_node，affected=0 即行不处于 pending（已被认领或已越过该阶段），
// 多实例并发认领同一行只有一个成功。
func (r *taskPersistentRepo) Claim(ctx context.Context, id int64, node string) (bool, error) {
	affected, err := r.client.TaskPersistent.Update().
		Where(
			taskpersistent.IDEQ(id),
			taskpersistent.PhaseEQ(int8(TaskPhasePending)),
		).
		SetPhase(int8(TaskPhaseDispatching)).
		SetClaimedNode(node).
		Save(ctx)
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (r *taskPersistentRepo) MarkDispatched(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	// 只更新 dispatching 行：阶段不匹配（对账已收敛/重复回调）幂等忽略
	_, err := r.client.TaskPersistent.Update().
		Where(
			taskpersistent.IDIn(ids...),
			taskpersistent.PhaseEQ(int8(TaskPhaseDispatching)),
		).
		SetPhase(int8(TaskPhaseDispatched)).
		Save(ctx)
	return err
}

func (r *taskPersistentRepo) MarkTerminal(ctx context.Context, id int64, outcome TaskOutcome, errSummary string) error {
	if outcome == TaskOutcomeUnset {
		return errors.New("states: terminal outcome must be set explicitly")
	}
	// dispatching 行也允许落终态（发布成功但未确认即崩溃的行，对账按 backend
	// 终态收敛）；pending/terminal 行忽略
	_, err := r.client.TaskPersistent.Update().
		Where(
			taskpersistent.IDEQ(id),
			taskpersistent.PhaseIn(int8(TaskPhaseDispatching), int8(TaskPhaseDispatched)),
		).
		SetPhase(int8(TaskPhaseTerminal)).
		SetOutcome(int8(outcome)).
		SetErrorSummary(errSummary).
		SetFinishedAt(time.Now()).
		Save(ctx)
	return err
}

func (r *taskPersistentRepo) Requeue(ctx context.Context, id int64) error {
	_, err := r.client.TaskPersistent.Update().
		Where(
			taskpersistent.IDEQ(id),
			taskpersistent.PhaseIn(int8(TaskPhaseDispatching), int8(TaskPhaseDispatched)),
		).
		SetPhase(int8(TaskPhasePending)).
		Save(ctx)
	return err
}

func (r *taskPersistentRepo) Inflight(ctx context.Context, olderThan time.Time, limit int) ([]*TaskPersistent, error) {
	rows, err := r.client.TaskPersistent.Query().
		Where(
			taskpersistent.PhaseIn(int8(TaskPhaseDispatching), int8(TaskPhaseDispatched)),
			taskpersistent.UpdatedAtLT(olderThan),
		).
		Order(ent.Asc(taskpersistent.FieldUpdatedAt)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*TaskPersistent, 0, len(rows))
	for _, e := range rows {
		out = append(out, taskPersistent(e))
	}
	return out, nil
}

// getByKey 按幂等键回查已有行，Create 冲突时使用。
func (r *taskPersistentRepo) getByKey(ctx context.Context, key string) (*TaskPersistent, error) {
	e, err := r.client.TaskPersistent.Query().
		Where(taskpersistent.IdempotencyKeyEQ(key)).
		First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, fmt.Errorf("%w: idempotency_key %s", ErrTaskNotFound, key)
		}
		return nil, err
	}
	return taskPersistent(e), nil
}

// taskPersistent 把 ent 实体映射为领域结构。
func taskPersistent(en *ent.TaskPersistent) *TaskPersistent {
	return &TaskPersistent{
		Id:             en.ID,
		Resource:       en.Resource,
		Operator:       en.Operator,
		Phase:          TaskPhase(en.Phase),
		Outcome:        TaskOutcome(en.Outcome),
		ErrorSummary:   en.ErrorSummary,
		TimeoutMs:      en.TimeoutMs,
		MaxAttempts:    en.MaxAttempts,
		ClaimedNode:    en.ClaimedNode,
		IdempotencyKey: en.IdempotencyKey,
		Payload:        en.Payload,
		Vpc:            en.Vpc,
		Node:           en.Node,
		Label:          en.Label,
		HashBucket:     en.HashBucket,
		FinishedAt:     en.FinishedAt,
		CreatedAt:      en.CreatedAt,
		UpdatedAt:      en.UpdatedAt,
	}
}
