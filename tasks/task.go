// Package tasks 定义 stateflux 的业务任务注册中心：一类任务一个 TaskSpec，
// 调度侧（listener/scheduler/publisher）与执行侧（queue worker）均以此为准。
package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/improvtrace/stateflux/states"
)

// DefaultConcurrency 是任务消费并发的缺省值：TaskSpec.Concurrency <=0 时
// worker 启动取用。
const DefaultConcurrency = 10

// defaultFactoryBatch 是 Factory 单批生成量的缺省值。
const defaultFactoryBatch = 100

// TaskSpec 是一类业务任务的完整描述：怎么执行（Handler）、以什么资源路由
// （Resource，队列按 资源×节点 划分）、失败怎么重试（Retries，由 queue 框架
// 承担）、是否需要可靠性保障（Durable，dispatch 前落 task_persistents）、
// 到点怎么生成（Factory）。一切路由以 Resource 为键；消费并发随本结构声明
// ——新增任务只需本结构，无需改框架。
type TaskSpec struct {
	Resource      string          // 任务资源名，= task_persistents.resource，全局唯一；路由 header AttributeResource
	SignatureName string          // handler 注册名（Signature.Name），空 = Resource；转发类任务的资源名与 handler 名不同时使用
	Concurrency   int             // 消费端并发数（<=0 取 DefaultConcurrency），本资源在本节点队列上的 worker 并发
	Handler       any             // 处理函数，约定签名 func(payload string, taskID int64) error，见 Validate
	Timeout       time.Duration   // 单次执行预算，<=0 时 NewRow 取 1 分钟
	Retries       int             // 执行内重试次数，映射 Signature.RetryCount，由 queue 框架执行
	MaxAttempts   int32           // 最大重试次数落表值（重试由异步任务框架实施），<=0 时 NewRow 取 3
	Durable       bool            // 是否落 task_persistents（dispatch 前持久化，支持重启恢复）；false = 无状态直发
	Payload       json.RawMessage // 非 Durable 定时任务的固定参数，直发时作为 payload 传入
	Cron          string          // 可选：cron 表达式（robfig/cron v3，五段或 @every 等描述符），到点触发
	FactoryBatch  int             // Factory 单批生成量，<=0 时取缺省 100
	Factory       Factory         // 可选：Durable 定时任务到点生成待发布批次
}

// HandlerName 返回 handler 注册名（Signature.Name）：未显式指定 SignatureName
// 时与 Resource 相同（资源名即 handler 名）。
func (s *TaskSpec) HandlerName() string {
	if s.SignatureName != "" {
		return s.SignatureName
	}
	return s.Resource
}

var (
	handlerPayloadType = reflect.TypeOf("")
	handlerTaskIDType  = reflect.TypeOf(int64(0))
	handlerErrorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// handlerOK 判断 Handler 是否符合约定签名
// func(payload string, taskID int64) error。payload 是发布约定的 JSON 字符串
// （见 payload.go）；taskID 由框架经第二个参数传入，Durable 任务非零，
// 无状态直发任务为 0。固定两参使得 entry 无需反射即可包装完成回写。
func handlerOK(h any) bool {
	t := reflect.TypeOf(h)
	return t != nil && t.Kind() == reflect.Func &&
		t.NumIn() == 2 && t.In(0) == handlerPayloadType && t.In(1) == handlerTaskIDType &&
		t.NumOut() == 1 && t.Out(0) == handlerErrorType
}

// Validate 校验必填项与互斥约束，NewRegistry 构造时调用。
func (s *TaskSpec) Validate() error {
	if s.Resource == "" {
		return errors.New("tasks: Resource is required")
	}
	if !handlerOK(s.Handler) {
		return fmt.Errorf("tasks: %s Handler must be func(payload string, taskID int64) error", s.Resource)
	}
	if s.Concurrency < 0 {
		return fmt.Errorf("tasks: %s concurrency must not be negative", s.Resource)
	}
	// Factory 产物落表（DispatchableTask.Task），仅 Durable 任务支持；
	// 定时触发的 Durable 任务必须由 Factory 生成批次，否则到点无事可做
	if s.Factory != nil && !s.Durable {
		return fmt.Errorf("tasks: %s is not durable and cannot use a Factory", s.Resource)
	}
	if s.Durable && s.Cron != "" && s.Factory == nil {
		return fmt.Errorf("tasks: durable cron task %s must configure a Factory", s.Resource)
	}
	return nil
}

// NewRow 以本 spec 的默认值构建一行待落表任务（Durable 任务用）：填
// Resource/TimeoutMs/MaxAttempts。ID（雪花）、IdempotencyKey 与业务属性由
// 调用方补。
func (s *TaskSpec) NewRow(payload json.RawMessage) *states.TaskPersistent {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = time.Minute
	}
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &states.TaskPersistent{
		Resource:    s.Resource,
		TimeoutMs:   timeout.Milliseconds(),
		MaxAttempts: maxAttempts,
		Payload:     payload,
	}
}

// Registry 是 TaskSpec 的注册中心，按 Resource 索引。构造后只读，并发安全。
type Registry struct {
	specs map[string]*TaskSpec
}

// NewRegistry 构造并注册一批任务定义，任一 Validate 失败、Resource 重复或
// handler 注册名（SignatureName）冲突即报错。
func NewRegistry(specs ...*TaskSpec) (*Registry, error) {
	r := &Registry{specs: make(map[string]*TaskSpec, len(specs))}
	names := make(map[string]string, len(specs))
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if _, ok := r.specs[spec.Resource]; ok {
			return nil, fmt.Errorf("tasks: duplicate resource %s", spec.Resource)
		}
		if prev, ok := names[spec.HandlerName()]; ok {
			return nil, fmt.Errorf("tasks: duplicate handler name %s (resources %s and %s)",
				spec.HandlerName(), prev, spec.Resource)
		}
		r.specs[spec.Resource] = spec
		names[spec.HandlerName()] = spec.Resource
	}
	return r, nil
}

// Get 按资源名查询任务定义。
func (r *Registry) Get(resource string) (*TaskSpec, bool) {
	spec, ok := r.specs[resource]
	return spec, ok
}

// Specs 返回全部任务定义，map 遍历无固定顺序。
func (r *Registry) Specs() []*TaskSpec {
	specs := make([]*TaskSpec, 0, len(r.specs))
	for _, spec := range r.specs {
		specs = append(specs, spec)
	}
	return specs
}

// Handlers 返回 handler 注册名（Signature.Name）→Handler 映射，entry 启动时
// 整体注册进 queue.Server。
func (r *Registry) Handlers() map[string]any {
	handlers := make(map[string]any, len(r.specs))
	for _, spec := range r.specs {
		handlers[spec.HandlerName()] = spec.Handler
	}
	return handlers
}

// Timed 返回配置了 Cron 的任务定义，定时调度器构建时用。
func (r *Registry) Timed() []*TaskSpec {
	var specs []*TaskSpec
	for _, spec := range r.specs {
		if spec.Cron != "" {
			specs = append(specs, spec)
		}
	}
	return specs
}
