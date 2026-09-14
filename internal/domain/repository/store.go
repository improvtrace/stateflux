package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// Store 是任务账本的组合根（§3.1/§5.2/§5.5）：聚合七张表的单表仓储（见下方访问器），并以本
// 文件的跨表方法承担唯一写面。晋升、认领、终态与 R1 重置都是「读源表 → 写目标表 → 删源表」的
// 挪行，必须落在同一个 PG 事务内，单表接口无法表达这种原子性，故由组合根统一提供。
//
// 为什么以访问器聚合而非直接内嵌各表接口：Go 要求接口内嵌时同名方法的签名必须一致，而七个单表
// 接口都声明了 Create/Get/DeleteByIDs（签名各不相同），直接内嵌无法通过编译。访问器保留单表
// 查询能力，同时把跨表写入收敛到本接口，避免调用方用单表接口拼出半截事务。
//
// 约定：跨表方法各自开事务并负责提交/回滚，调用方不要再用 WithTx 包裹它们。
type Store interface {
	Transaction

	// TaskPendings 返回 task_pendings 单表仓储（§5.1/§5.2）。
	TaskPendings() TaskPendingsRepository
	// TaskSchedulables 返回 task_schedulables 单表仓储（§5.2）。
	TaskSchedulables() TaskSchedulablesRepository
	// TaskProcessings 返回 task_processings 单表仓储（§5.2/§6.2）。
	TaskProcessings() TaskProcessingsRepository
	// TaskCompleteds 返回 task_completeds 单表仓储（§5.5/§5.6）。
	TaskCompleteds() TaskCompletedsRepository
	// TaskPayloads 返回 task_payloads 单表仓储（§3.1/§5.5）。
	TaskPayloads() TaskPayloadsRepository
	// TaskResults 返回 task_results 单表仓储（§3.1/§5.5）。
	TaskResults() TaskResultsRepository
	// TaskIdentities 返回 task_identities 单表仓储（§3.1/§5.1）。
	TaskIdentities() TaskIdentitiesRepository

	// Enqueue 在一个 PG 事务内写 identity、payload 与 pending（§5.1）。幂等键命中时返回账本中
	// 已有的 task_id，Created=false，且不重复写 payload/pending。
	Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error)
	// Promote 批量 pending → schedulable 并删除源行，按 priority DESC 取候选（§5.2）。
	Promote(ctx context.Context, limit int) (int, error)
	// Claim 以单条原生 SQL（FOR UPDATE SKIP LOCKED）把 schedulable → processing，attempts +1，
	// 返回本次认领的在途行（§5.2）。attempt 即执行 fence，只在 claim 时递增。
	Claim(ctx context.Context, limit int) ([]Claimed, error)
	// Complete 校验 attempt fence 后把 processing 迁移到终态：写 completed、写 task_results
	// 墓碑、删 payload、删 processing，全部在一个事务内（§5.5）。attempt 不匹配或行已不存在时
	// 返回 false 且不产生任何副作用；墓碑重复同样返回 false。
	Complete(ctx context.Context, req CompleteRequest) (bool, error)
	// ResetExpired 是 R1 对账：updated_at 早于 deadline 的 processing 行移回 schedulable 并删除
	// processing，不改动 attempts，重置后立即重新可认领（§6.2）。返回重置行数。
	ResetExpired(ctx context.Context, deadline time.Time, limit int) (int, error)
	// DeadLetter 是 R3 死信终态：把 processing 行以 outcome=dead 迁入 completed（§6.2）。
	DeadLetter(ctx context.Context, taskID int64) (bool, error)

	// GetPayload 按 task_id 读取在途 payload（构造 TaskMessage，§3.3）。
	GetPayload(ctx context.Context, taskID int64) ([]byte, error)
	// Requeue 可重试失败：processing → schedulable，不修改 attempts（下次 claim 才 +1，§5.5）。
	Requeue(ctx context.Context, taskID int64) (bool, error)
	// GetProcessing 按 task_id 读取在途行（不存在返回 ent.NotFoundError）。
	GetProcessing(ctx context.Context, taskID int64) (*Processing, error)
	// GetResult 按 task_id 读取终态结果（不存在返回 ent.NotFoundError）。
	GetResult(ctx context.Context, taskID int64) (*Result, error)
	// CountPending 返回 pending 待晋升行数。
	CountPending(ctx context.Context) (int, error)
	// CountSchedulable 返回 schedulable 就绪行数。
	CountSchedulable(ctx context.Context) (int, error)
	// CountProcessing 返回 processing 在途行数。
	CountProcessing(ctx context.Context) (int, error)
}

// Processing 是在途任务行的只读投影（§3.1/§5.2）：只携带调度、分发与观测所需字段，避免把
// ent 实体泄漏进组合根契约。Claim 与 GetProcessing 都返回它。
type Processing struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
	Attempt        int64 // 本次认领后的 attempts，即执行 fence（§5.5/§6.2）
	ClaimedNode    string
	IdempotencyKey string
	Callback       json.RawMessage
	ParentTaskID   int64
	Vpc            string
	Node           string
	Label          string
	HashBucket     int16
	BizRaceLabels  []string
	BizRaceEntry   string
	BizGroup       string
	BizBatchID     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Claimed 是 Processing 的别名，强调该行来自 claim 挪行（§5.2）。
type Claimed = Processing

// EnqueueRequest 是一次创建请求（§5.1）：公共段字段 + payload。TaskID 由调用方以雪花生成；
// IdempotencyKey 为空时跳过身份账本（不去重），否则由 task_identities 裁决幂等。
type EnqueueRequest struct {
	TaskID         int64
	Type           string
	Operator       string
	Priority       schema.Priority
	Channel        string
	TimeoutMs      int64
	MaxAttempts    int32
	IdempotencyKey string
	Callback       json.RawMessage
	ParentTaskID   int64
	Vpc            string
	Node           string
	Label          string
	HashBucket     int16
	BizRaceLabels  []string
	BizRaceEntry   string
	BizGroup       string
	BizBatchID     string
	Payload        json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// EnqueueResult 是创建结果：TaskID 为最终任务 ID（dedupe 命中时为原 ID），Created 标记是否新建。
type EnqueueResult struct {
	TaskID  int64
	Created bool
}

// CompleteRequest 是一次终态归集请求（§5.5）。Attempt 是结果携带的 fence，必须与 processing 行
// 的 attempts 相等，否则按陈旧结果拒绝且无副作用。
type CompleteRequest struct {
	TaskID      int64
	Attempt     int64
	Outcome     schema.Outcome
	Result      json.RawMessage
	Error       string
	CompletedAt time.Time
}

// Result 是 task_results 的只读投影（§5.5）：payload 与业务结果在终态事务内合并入行。
type Result struct {
	TaskID      int64
	Outcome     schema.Outcome
	Attempt     int64
	Payload     json.RawMessage
	Result      json.RawMessage
	Error       string
	CompletedAt time.Time
}
