package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/task"
)

// Result 是 handler 的执行结果（在 Runtime 内组装为 ResultEvent）。
type Result struct {
	// Outcome 是终态类别（succeeded / failed / dead；unset 视为 failed）。
	Outcome taskv1.Outcome
	// Payload 是业务结果字节（可空）。
	Payload []byte
	// Error 是失败摘要（成功时为空）。
	Error string
}

// Handler 是任务执行处理器（§5.4）：由 biz 在装配期实现并注册（§15.1#2/#12）。
type Handler interface {
	// Type 是任务类型（粗）。
	Type() string
	// Operator 是任务子类型（细）；二者共同决定 handler 选择（§3.1）。
	Operator() string
	// Handle 执行任务并返回结果。返回 error 表示执行本身失败（会被标记为 failed，
	// 是否重试由调度侧按语义与 max_attempts 决定，§5.3）。
	Handle(ctx context.Context, msg *taskv1.TaskMessage) (Result, error)
}

// HandlerFunc 是 Handler 的函数适配器。
type HandlerFunc struct {
	T string
	O string
	F func(ctx context.Context, msg *taskv1.TaskMessage) (Result, error)
}

// Type 实现 Handler。
func (h HandlerFunc) Type() string { return h.T }

// Operator 实现 Handler。
func (h HandlerFunc) Operator() string { return h.O }

// Handle 实现 Handler。
func (h HandlerFunc) Handle(ctx context.Context, msg *taskv1.TaskMessage) (Result, error) {
	if h.F == nil {
		return Result{}, errors.New("worker: nil handler function")
	}
	return h.F(ctx, msg)
}

var _ Handler = HandlerFunc{}

// HandlerRegistry 是执行侧 handler 注册表（§5.4）：装配期注册、运行期只读。
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[task.Signature]Handler
}

// NewHandlerRegistry 创建空注册表。
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: map[task.Signature]Handler{}}
}

// Register 注册 handler；空 type、nil 或重名返回错误。
func (r *HandlerRegistry) Register(h Handler) error {
	if h == nil {
		return errors.New("worker: nil handler")
	}
	if h.Type() == "" {
		return errors.New("worker: handler type must not be empty")
	}
	sig := task.SignatureOf(h.Type(), h.Operator())
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[sig]; ok {
		return fmt.Errorf("worker: duplicate handler %q", sig)
	}
	r.handlers[sig] = h
	return nil
}

// Resolve 按 type/operator 解析 handler。
func (r *HandlerRegistry) Resolve(taskType, operator string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[task.SignatureOf(taskType, operator)]
	return h, ok
}

// Signatures 返回已注册 handler 签名。
func (r *HandlerRegistry) Signatures() []task.Signature {
	r.mu.RLock()
	out := make([]task.Signature, 0, len(r.handlers))
	for sig := range r.handlers {
		out = append(out, sig)
	}
	r.mu.RUnlock()
	return out
}
