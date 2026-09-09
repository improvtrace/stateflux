// Package store 是权威存储抽象（§3.1/§8）：生命周期四阶段首先是逻辑抽象——
// Pending / Schedulable / Processing / Completed 四个阶段集合，由本包以统一接口暴露
// （创建入集、约束晋升、认领挪行、终态搬移、对账扫描）。
//
// 默认实现 = ent + PG 四阶段分区表 + payload 分离表：生命周期阶段由所在表表达（status 列取消），
// 整体可替换（如退回单表 + 状态列）而不改变调度与执行语义。
// 关键并发路径（幂等创建、晋升挪行、SKIP LOCKED 认领、终态事务）以 ent 原生 SQL 下沉，
// 不依赖 ORM 生成的隐式语句；常规查询走 ent 生成的类型安全 API。
package store

import (
	"context"
	"errors"
	"time"

	"github.com/improvtrace/stateflux/sdk"
)

// 阶段集合语义错误。
var (
	// ErrNotFound 请求的任务不存在。
	ErrNotFound = errors.New("store: task not found")
	// ErrEmptyBatch 空批量。
	ErrEmptyBatch = errors.New("store: empty batch")
	// ErrInvalidTask 任务字段非法（type 为空 / payload 非 JSON / 枚举非法）。
	ErrInvalidTask = errors.New("store: invalid task")
)

// CreatedTask 创建结果：Duplicate = idempotency_key 冲突，跳过插入并返回已存在任务 ID（§5.1）。
type CreatedTask struct {
	TaskID    int64
	Duplicate bool
}

// PromoteOptions 约束晋升参数（§5.2）。
type PromoteOptions struct {
	// Now 当前时间（run_at 到期判定基准）。
	Now time.Time
	// Limit 本轮扫描批量。
	Limit int
	// TypeConcurrency 内置约束：per-type 全局并发上限，按 processing_tasks 在途计数判定
	//（0 或缺省 = 不限）。
	TypeConcurrency map[string]int
	// Preconditions 业务约束钩子（按任务类型注册，§5.2）。
	Preconditions map[string]sdk.Precondition
}

// PromoteStats 晋升统计。
type PromoteStats struct {
	Scanned                   int
	Promoted                  int
	BlockedConcurrency        int
	BlockedPrecondition       int
	BlockedPreconditionByType map[string]int
}

// TerminalAction 终态批处理内单条结果的路由（§5.5）：
// 终态搬移（processing → completed + task_results + 回调派生）或重试（processing → schedulable）。
type TerminalAction int

const (
	// TerminalActionTerminal 按结果落终态（succeeded/failed）。
	TerminalActionTerminal TerminalAction = iota
	// TerminalActionRetry 可重试失败且未耗尽 attempts：挪回 schedulable（attempt 不变，
	// run_at = 退避后时间；约束已通过不重评，§5.5）。
	TerminalActionRetry
)

// TerminalEntry 归集批处理的单条输入。Attempt 是 fencing token：与 processing 行的
// attempts 匹配才生效，重复搬移/陈旧结果无副作用（幂等）。
type TerminalEntry struct {
	TaskID      int64
	Attempt     int64
	Action      TerminalAction
	Outcome     sdk.Outcome // Terminal：succeeded/failed
	Result      []byte      // Terminal：业务结果（jsonb；nil 表示无结果）
	Error       string      // Terminal：错误摘要
	CompletedAt time.Time
	RetryRunAt  time.Time // Retry：run_at = now + 退避（调用方以 sdk.Backoff 计算）
}

// EntryStatus 单条结果的处理结果。
type EntryStatus int

const (
	// EntryStatusApplied 生效（attempt 匹配，挪行/落库完成）。
	EntryStatusApplied EntryStatus = iota
	// EntryStatusSkipped 跳过：attempt 不匹配（陈旧副本/重复归集）或任务已不在 processing。
	EntryStatusSkipped
)

// FinalizeReport 终态批处理报告。
type FinalizeReport struct {
	// Applied / Skipped：与输入顺序无关，按 task_id 归集。
	Applied map[int64]EntryStatus
	// Derived 派生的回调任务 ID（§5.8，与父任务终态同事务写入）。
	Derived []int64
}

// RequeueEntry 对账重置（R1）的单条输入（§6.3）：不携带结果，仅挪回 schedulable；
// attempts 耗尽时由 store 直接路由为 dead（R3）。
type RequeueEntry struct {
	TaskID  int64
	Attempt int64
	RunAt   time.Time // now + 退避（调用方以 sdk.Backoff 计算）
	// Error 重置原因（attempts 耗尽进 dead 时写入 error 摘要）。
	Error string
}

// RequeueReport 对账重置报告。
type RequeueReport struct {
	Requeued int // 挪回 schedulable（含本轮路由为 dead 之外的）
	Dead     int // R3：attempts >= max_attempts → completed{dead}
	Skipped  int // attempt 不匹配 / 已终态
}

// DeadTask 死信行（§12.9 运维面）。
type DeadTask struct {
	Task        sdk.Task
	Outcome     sdk.Outcome
	Payload     []byte
	CompletedAt time.Time
}

// Store 阶段集合逻辑接口（§3.1）。所有多行操作批量化；幂等语义见各方法。
type Store interface {
	// Migrate 装配存储：ent 迁移建表 + 幂等键局部唯一索引 + NOTIFY 触发器（幂等 DDL）。
	Migrate(ctx context.Context) error

	// CreatePending 创建入集（§5.1，批处理）：单事务批量 INSERT pending_tasks + task_payloads；
	// idempotency_key 冲突跳过插入、返回已存在 task_id。只写 PG，不触碰 Redis。
	CreatePending(ctx context.Context, items []sdk.NewTask) ([]CreatedTask, error)

	// Promote 约束晋升（§5.2）：扫描 pending（run_at 到期）→ 内置并发约束 + 业务约束钩子评估
	// 通过 → 批量挪入 schedulable。未通过的任务留在 pending，等待下轮评估。
	Promote(ctx context.Context, opts PromoteOptions) (PromoteStats, error)

	// Claim 认领挪行（§5.3）：schedulable → processing 的单条 SQL 挪行
	//（ORDER BY ... FOR UPDATE SKIP LOCKED），查询与迁移原子完成，天然防重复认领；
	// attempts +1（fencing token）。返回的任务内联 payload（构建 TaskMessage 用）。
	Claim(ctx context.Context, nodeID string, limit int) ([]*sdk.Task, error)

	// Finalize 终态批处理（§5.5）：每条 entry 按 Action 路由——
	// 终态搬移（processing → completed 挪行，payload 合并 + task_results 一次性写入
	// ON CONFLICT DO NOTHING + 回调派生，全部同一事务）或重试回 schedulable。
	// 整批单事务；attempt 匹配才生效。
	Finalize(ctx context.Context, entries []TerminalEntry) (*FinalizeReport, error)

	// Requeue 对账重置（§6.3 R1/R3）：processing → schedulable（run_at = 退避）；
	// attempts >= max_attempts → completed{dead}（R3，同事务，payload 合并 + task_results）。
	Requeue(ctx context.Context, entries []RequeueEntry) (*RequeueReport, error)

	// GetResults 结果查询（§5.6）：先查 task_results（终态即命中返回），未命中查阶段表
	// 判断在途状态。stages 覆盖全部请求 ID。
	GetResults(ctx context.Context, taskIDs []int64) (results map[int64]*sdk.TaskResult, stages map[int64]sdk.Stage, err error)

	// ScanProcessing 对账扫描：updated_at 早于 cutoff 的 processing 任务（§6.3 R1 候选）。
	ScanProcessing(ctx context.Context, updatedBefore time.Time, limit int) ([]*sdk.Task, error)

	// ExistsProcessing 判断哪些 task_id 仍在 processing（R2 幽灵清理用）。
	ExistsProcessing(ctx context.Context, taskIDs []int64) (map[int64]bool, error)

	// ListDead 死信查询（§12.9）：outcome=dead，按 id 倒序分页；taskType 为空不过滤；
	// cursor 为上一页返回的 next_cursor（0 表示从头）。
	ListDead(ctx context.Context, taskType string, limit int, cursor int64) ([]DeadTask, int64, error)

	// Redrive 死信重跑（§12.9）：人工修复后重跑——按原 payload 复制创建全新任务实例
	//（新雪花 ID、attempts 归零、不自动重放）。
	Redrive(ctx context.Context, taskIDs []int64) ([]int64, error)

	// LatestSuccessAt 工厂支撑（§5.7）：batch_id 前缀匹配的最新成功完成时间。
	// next = last_success + period 现算，不持久化绝对时间点。
	LatestSuccessAt(ctx context.Context, batchPrefix string) (time.Time, bool, error)

	// HasDeadByBatchPrefix 工厂孤儿判定（§5.7）：前缀下存在 dead 任务即冻结条目。
	HasDeadByBatchPrefix(ctx context.Context, batchPrefix string) (bool, error)

	// Close 释放底层连接。
	Close() error
}
