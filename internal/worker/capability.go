// Package worker 是执行侧运行时（§5.4、§8）。按 §15.1#8 的确认，本包**不实现**
// api/stateflux/worker/v1 的 RPC——那是 internal/biz 的职责；本包只负责组织、注册与
// 管理「执行节点能力」，并提供执行运行时的公共机制（任务订阅/执行/结果发布）。
//
// 能力模型：biz 在装配期把能力实现注册进 Registry；biz 的 CapabilityService 适配器
// 按名字从本 Registry 取能力并调用。这样「能力定义在 api/、实现在 biz、组织在本包」
// 三者职责分离（§15.1#7/#8）。
package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Descriptor 描述一个已注册能力（与 api/stateflux/worker/v1.CapabilityDescriptor 对应）。
type Descriptor struct {
	Name        string
	Description string
	Labels      []string
	Async       bool
}

// Request 是能力调用的通用信封（与 worker/v1.InvokeRequest 对应）。
type Request struct {
	Name     string
	Payload  []byte
	Metadata map[string]string
}

// Response 是能力调用结果（与 worker/v1.InvokeResponse 对应）。
type Response struct {
	Payload []byte
	Error   string
}

// Capability 是执行节点能力（§15.1#7）：无状态、可随时增减。实现放 internal/biz。
type Capability interface {
	// Descriptor 返回能力描述（注册与管理面据此发现）。
	Descriptor() Descriptor
	// Invoke 执行能力。返回 error 表示执行失败，Response.Error 用于向调用方传递业务错误。
	Invoke(ctx context.Context, req Request) (Response, error)
}

// Func 是 Capability 的函数适配器。
type Func struct {
	D Descriptor
	F func(ctx context.Context, req Request) (Response, error)
}

// Descriptor 实现 Capability。
func (f Func) Descriptor() Descriptor { return f.D }

// Invoke 实现 Capability。
func (f Func) Invoke(ctx context.Context, req Request) (Response, error) {
	if f.F == nil {
		return Response{}, errors.New("worker: nil capability function")
	}
	return f.F(ctx, req)
}

var _ Capability = Func{}

// Registry 是能力注册表（§15.1#8）：装配期注册、运行期只读。
type Registry struct {
	mu    sync.RWMutex
	caps  map[string]Capability
	order []string
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{caps: map[string]Capability{}}
}

// Register 注册能力；空名、nil 或重名返回错误。
func (r *Registry) Register(c Capability) error {
	if c == nil {
		return errors.New("worker: nil capability")
	}
	name := c.Descriptor().Name
	if name == "" {
		return errors.New("worker: capability name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.caps[name]; ok {
		return fmt.Errorf("worker: duplicate capability %q", name)
	}
	r.caps[name] = c
	r.order = append(r.order, name)
	sort.Strings(r.order)
	return nil
}

// Get 按名解析能力。
func (r *Registry) Get(name string) (Capability, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.caps[name]
	return c, ok
}

// Has 判断能力是否已注册。
func (r *Registry) Has(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// Descriptors 返回全部能力描述（按名稳定排序）。
func (r *Registry) Descriptors() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.order))
	for _, name := range r.order {
		out = append(out, r.caps[name].Descriptor())
	}
	return out
}

// Names 返回全部能力名。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.order...)
}

// Invoke 按名字调用能力：未注册返回错误。它是 biz 适配器的唯一入口，
// 保证「能力组织」集中在本包（§15.1#8）。
func (r *Registry) Invoke(ctx context.Context, req Request) (Response, error) {
	c, ok := r.Get(req.Name)
	if !ok {
		return Response{}, fmt.Errorf("worker: unregistered capability %q", req.Name)
	}
	resp, err := c.Invoke(ctx, req)
	if err != nil {
		return Response{}, err
	}
	return resp, nil
}
