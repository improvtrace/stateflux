// Package sdk 是业务方唯一依赖面（§8）：任务与结果的类型、生命周期阶段、Handler 接口、
// 回调规格与重试退避。其余包（scheduler/executor/collector 等）为引擎内部，业务方不直接引用。
package sdk

import (
	"encoding/json"
	"fmt"
	"time"
)

// Priority 任务优先级，与 Redis 就绪队列 stateflux:queue:{pri} 的三级拆分对应（§3.2）。
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Valid 报告优先级是否合法。
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// Normalize 把空值归一为 normal。
func (p Priority) Normalize() Priority {
	if p == "" {
		return PriorityNormal
	}
	return p
}

// ExecMode 执行方式（§1.3）：sync = 调度节点调用执行节点 RPC 并等结果（结果需及时回执）；
// async = 调度节点投递 Redis 队列、执行节点消费执行。
type ExecMode string

const (
	ExecSync  ExecMode = "sync"
	ExecAsync ExecMode = "async"
)

// Valid 报告执行方式是否合法。
func (m ExecMode) Valid() bool {
	switch m {
	case ExecSync, ExecAsync:
		return true
	}
	return false
}

// Normalize 把空值归一为 async。
func (m ExecMode) Normalize() ExecMode {
	if m == "" {
		return ExecAsync
	}
	return m
}

// Stage 生命周期阶段集合（§3.1 逻辑抽象，默认实现为 PG 四阶段分区表，由所在表表达）。
type Stage string

const (
	StagePending     Stage = "pending"
	StageSchedulable Stage = "schedulable"
	StageProcessing  Stage = "processing"
	StageCompleted   Stage = "completed"
	// StageUnknown 表示任务不存在于任何阶段表（查询无果时使用）。
	StageUnknown Stage = "unknown"
)

// Outcome 任务终态结果（§3.1 completed_tasks.outcome）。
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	// OutcomeDead 表示重试/对账重置耗尽 attempts（§6.3 R3）。
	OutcomeDead Outcome = "dead"
	// OutcomeNone 表示尚未终态。
	OutcomeNone Outcome = ""
)

// Terminal 报告是否终态。
func (o Outcome) Terminal() bool {
	return o == OutcomeSucceeded || o == OutcomeFailed || o == OutcomeDead
}

// 重试预算默认值：max_attempts 未显式指定时使用（含首次执行）。
const DefaultMaxAttempts = 3

// 任务执行超时默认值（毫秒）。
const DefaultTimeoutMS = 60000

// Task 任务的完整内存表示。跨阶段携带，store 实现负责与 PG 行互转。
type Task struct {
	ID       int64
	Type     string
	Payload  []byte
	Priority Priority
	ExecMode ExecMode
	// RunAt 最早可调度时间：承载延迟任务与重试退避（§3.1）。
	RunAt       time.Time
	TimeoutMS   int64
	MaxAttempts int32
	Attempts    int64
	// OwnerNode 认领该任务的调度节点。
	OwnerNode string
	// Error 终态错误摘要（完整结果在 TaskResult，§3.1）。
	Error string
	// IdempotencyKey 业务幂等键（§5.1 两模式幂等语义一致）。
	IdempotencyKey string
	// BatchID 批量创建归组（工厂任务据此冻结孤儿，§5.7）。
	BatchID string
	// Callback OnSuccess/OnError 回调规格（§5.8），JSON 序列化存于任务行 callback 字段。
	Callback *CallbackSpec
	// ParentTaskID 回调派生溯源。
	ParentTaskID int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// NewTask 创建入参（创建阶段统一入口，模式 A 直写与模式 B RPC 共用同一语义）。
type NewTask struct {
	Type           string
	Payload        []byte
	Priority       Priority
	ExecMode       ExecMode
	RunAt          time.Time
	TimeoutMS      int64
	MaxAttempts    int32
	IdempotencyKey string
	BatchID        string
	Callback       *CallbackSpec
	ParentTaskID   int64
}

// TaskResult 任务结果集行（task_results，§3.1）：终态事务内一次性写入、不可变；
// 业务结果的保留期与 completed 归档策略解耦。
type TaskResult struct {
	TaskID      int64
	Outcome     Outcome
	Attempt     int64
	Result      []byte
	Error       string
	CompletedAt time.Time
}

// CallbackSpec 任务行 callback 字段的存储规格（§5.8，借鉴 Machinery callback 核心语义）。
// 任务的 callback 字段只关心 OnSuccess / OnError 两个子规格；每个子规格本身又是一个
// CallbackSpec（其 OnSuccess/OnError 描述「本回调任务完成后再派生什么」，嵌套覆盖链式场景，
// 深度上限 MaxCallbackDepth）。
type CallbackSpec struct {
	// Type 任务类型（Handler 注册名）。仅作为 OnSuccess/OnError 子规格时必须非空；
	// 任务行顶层的 callback 容器只使用 OnSuccess/OnError 字段。
	Type string `json:"type,omitempty"`
	// PayloadTemplate 回调任务 payload 模板。注入规则：
	//   OnSuccess —— 模板 JSON 对象注入 "parent_result" 字段（父任务 result 解析为 JSON，
	//   解析失败则保留原字符串）；模板非对象时以 {"parent_result": ...} 整体替换。
	//   OnError —— 同理注入 "parent_error" 字段（错误摘要字符串）。
	PayloadTemplate json.RawMessage `json:"payload_template,omitempty"`
	// OnSuccess / OnError 嵌套规格：本回调任务完成后再派生的后续任务。
	OnSuccess *CallbackSpec `json:"on_success,omitempty"`
	OnError   *CallbackSpec `json:"on_error,omitempty"`
}

// MaxCallbackDepth 回调规格嵌套深度上限（§5.8）。
const MaxCallbackDepth = 8

// Validate 校验回调规格：OnSuccess/OnError 子规格必须带 Type，嵌套深度不超上限。
func (c *CallbackSpec) Validate() error {
	return validateCallback(c, 1)
}

func validateCallback(c *CallbackSpec, depth int) error {
	if c == nil {
		return nil
	}
	if depth > MaxCallbackDepth {
		return fmt.Errorf("callback nesting depth exceeds limit %d", MaxCallbackDepth)
	}
	// 顶层容器（depth == 1）只承担 OnSuccess/OnError；子规格自身必须是可执行任务。
	if depth > 1 && c.Type == "" {
		return fmt.Errorf("callback type is required (depth %d)", depth)
	}
	if err := validateCallback(c.OnSuccess, depth+1); err != nil {
		return err
	}
	return validateCallback(c.OnError, depth+1)
}

// Marshal 序列化为任务行 callback 字段的存储格式（jsonb）。
func (c *CallbackSpec) Marshal() ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	return json.Marshal(c)
}

// UnmarshalCallback 从任务行 callback 字段反序列化。
func UnmarshalCallback(data []byte) (*CallbackSpec, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var c CallbackSpec
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("unmarshal callback spec: %w", err)
	}
	return &c, nil
}
