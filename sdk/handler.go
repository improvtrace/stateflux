package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Handler 业务执行器（§7）：业务方实现本接口并注册到 executor（或嵌入进程的 executor 角色），
// 框架负责生命周期与超时注入。Execute 返回的 []byte 作为任务结果原样透传至 task_results。
type Handler interface {
	// Type 任务类型（注册名，任务创建时的 type 字段匹配）。
	Type() string
	// Execute 执行任务。ctx 在任务超时（TimeoutMS）与执行节点优雅退出时被取消；
	// task.Payload 为任务载荷，task.Attempts 为当前是第几次执行（fencing token）。
	Execute(ctx context.Context, task *Task) ([]byte, error)
}

// HandlerFunc 函数式 Handler 适配器。
type HandlerFunc struct {
	HandlerType string
	Exec        func(ctx context.Context, task *Task) ([]byte, error)
}

// Type 实现 Handler。
func (f *HandlerFunc) Type() string { return f.HandlerType }

// Execute 实现 Handler。
func (f *HandlerFunc) Execute(ctx context.Context, task *Task) ([]byte, error) {
	return f.Exec(ctx, task)
}

// ErrHandlerNotFound 请求的任务类型未注册。
var ErrHandlerNotFound = errors.New("sdk: handler not registered")

// Precondition 业务约束钩子（§5.2）：业务通过 sdk 注册（任务类型 + 判定函数），
// 约束晋升前由框架调用；返回 false（或 error）则任务留在 pending，等待下轮评估。
type Precondition func(ctx context.Context, task *Task) (bool, error)

// Registry Handler 注册表（executor 持有）。并发安全。
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry 构造空注册表。
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register 注册 Handler；重复类型直接覆盖（便于热更新与测试）。
func (r *Registry) Register(h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[h.Type()] = h
}

// Lookup 查找 Handler。
func (r *Registry) Lookup(taskType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrHandlerNotFound, taskType)
	}
	return h, nil
}

// Types 返回全部已注册类型。
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}
