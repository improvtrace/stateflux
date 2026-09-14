package cacheview

import (
	"context"
	"time"
)

// State 是任务在**异步执行视图**中的状态（§15.1#9）。它是 Redis 中的派生视图，
// 不是权威状态：权威只由 PG 四阶段账本决定（§1.2.1）。视图丢失只会让调度侧
// 暂时看不到在途情况，由 R2 清理、由 R1 按 PG 收敛。
type State string

const (
	// StateDispatched 已投递（异步队列已入队，等待执行节点消费）。
	StateDispatched State = "dispatched"
	// StateProcessing 已被执行节点取走、正在执行。
	StateProcessing State = "processing"
	// StateSucceeded 执行成功（结果已发布）。
	StateSucceeded State = "succeeded"
	// StateFailed 执行失败（结果已发布）。
	StateFailed State = "failed"
)

// TaskState 是单个任务的异步执行视图记录。
type TaskState struct {
	TaskID    int64
	Attempt   int64
	State     State
	NodeID    string
	Queue     string
	Error     string
	UpdatedAt time.Time
}

// Options 是 cacheview 的键空间与 TTL 配置。Prefix 为空时使用默认前缀，
// 保证多实例共享同一键空间。
type Options struct {
	// Prefix 键前缀（如 stateflux:cacheview:）。
	Prefix string
	// TaskTTL 任务视图保留时长；<=0 用 DefaultTTL。
	TaskTTL time.Duration
	// DedupeTTL exactly_once 入口去重窗口；<=0 用 DefaultTTL。
	DedupeTTL time.Duration
	// RouteTTL 队列↔节点映射缓存 TTL。
	RouteTTL time.Duration
}

// DefaultTTL 是视图记录的缺省保留时长。
const DefaultTTL = 10 * time.Minute

// DefaultPrefix 是缺省键前缀。
const DefaultPrefix = "stateflux:cacheview:"

func (o Options) withDefaults() Options {
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}
	if o.TaskTTL <= 0 {
		o.TaskTTL = DefaultTTL
	}
	if o.DedupeTTL <= 0 {
		o.DedupeTTL = DefaultTTL
	}
	if o.RouteTTL <= 0 {
		o.RouteTTL = DefaultTTL
	}
	return o
}

// View 是任务异步执行状态的逻辑操作封装（§15.1#9）：任务状态、入口去重、
// 节点在途计数与队列路由缓存。全部方法在 Redis 不可用时允许返回错误——
// 调用方不得据此改变正确性判断（§6.2 R2）。
type View interface {
	// SetTaskState 写入/覆盖任务异步执行状态。
	SetTaskState(ctx context.Context, st TaskState) error
	// GetTaskState 读取任务异步执行状态；ok=false 表示视图缺失。
	GetTaskState(ctx context.Context, taskID int64) (TaskState, bool, error)
	// DeleteTaskState 删除任务视图（终态后调用）。
	DeleteTaskState(ctx context.Context, taskID int64) error

	// ClaimDedupe 是 exactly_once 的入口去重：首次返回 true，重复返回 false（§15.3#2）。
	// Redis 不可用时返回错误，调用方必须回落到 PG 幂等账本而不是放行重复投递。
	ClaimDedupe(ctx context.Context, key string) (bool, error)
	// ReleaseDedupe 释放去重标记（投递被证明失败时允许重试）。
	ReleaseDedupe(ctx context.Context, key string) error

	// IncrInflight / DecrInflight 维护节点在途任务计数（容量提示，§6.3）。
	IncrInflight(ctx context.Context, nodeID string) (int64, error)
	DecrInflight(ctx context.Context, nodeID string) (int64, error)
	// Inflight 读取节点在途计数。
	Inflight(ctx context.Context, nodeID string) (int64, error)

	// SetQueueRoute 缓存「异步队列 → 节点」映射（coherence 的物化视图，§15.1#4）。
	SetQueueRoute(ctx context.Context, queue, nodeID string) error
	// GetQueueRoute 读取队列归属；ok=false 表示视图缺失。
	GetQueueRoute(ctx context.Context, queue string) (string, bool, error)
	// QueueRoutes 返回全部队列路由快照。
	QueueRoutes(ctx context.Context) (map[string]string, error)

	// Purge 删除前缀下的全部视图（R2 清理，§6.2）：返回删除键数。Prefix 为空使用选项前缀。
	Purge(ctx context.Context, prefix string) (int64, error)
}
