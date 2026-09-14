// Package factory 是周期任务生成运行时（TaskFactory，§5.6、§15.1#12）：定义 Factory
// 接口与注册表，并提供按各自周期驱动的 Manager。Factory 的具体实现由 internal/biz
// 提供并在装配期注册（§15.1#12）；本包不 import biz。
//
// 纪律：Factory 只在当选的调度节点运行；错过周期窗口跳过（不补跑）；生成的任务一次性
// 创建，批次记入 biz_batch_id 供孤儿判定（§5.6）。生成动作经注入的 Sink 落到创建路径。
package factory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/internal/task"
)

// Factory 是周期任务工厂（§5.6）。
type Factory interface {
	// Name 是工厂唯一名（注册键）。
	Name() string
	// Interval 是生成周期。
	Interval() time.Duration
	// Generate 生成本周期任务；返回空切片表示本周期无任务（跳过，不补跑）。
	Generate(ctx context.Context, now time.Time) ([]task.Task, error)
}

// Sink 把生成的任务写入创建路径（由装配层提供：PG enqueue + payload，§5.1）。
type Sink func(ctx context.Context, t task.Task) error

// Registry 是工厂注册表：装配期由 biz 注册，运行期只读。
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{factories: map[string]Factory{}}
}

// Register 注册工厂；空名、nil 或重名返回错误。
func (r *Registry) Register(f Factory) error {
	if f == nil {
		return errors.New("task/factory: nil factory")
	}
	name := f.Name()
	if name == "" {
		return errors.New("task/factory: factory name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.factories[name]; ok {
		return fmt.Errorf("task/factory: duplicate factory %q", name)
	}
	r.factories[name] = f
	return nil
}

// Get 按名解析工厂。
func (r *Registry) Get(name string) (Factory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[name]
	return f, ok
}

// All 返回全部工厂（按名稳定排序）。
func (r *Registry) All() []Factory {
	r.mu.RLock()
	out := make([]Factory, 0, len(r.factories))
	for _, f := range r.factories {
		out = append(out, f)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Manager 按各工厂自己的周期驱动生成，并把产出交给 Sink。
type Manager struct {
	registry *Registry
	sink     Sink
	clock    func() time.Time
}

// NewManager 构造管理器；sink 为 nil 时 panic（生成的任务必须能落地）。
func NewManager(registry *Registry, sink Sink) *Manager {
	if sink == nil {
		panic("task/factory: nil sink")
	}
	return &Manager{registry: registry, sink: sink, clock: time.Now}
}

// WithClock 覆盖时钟（测试用）。
func (m *Manager) WithClock(clock func() time.Time) *Manager {
	if clock != nil {
		m.clock = clock
	}
	return m
}

// RunOnce 驱动全部工厂生成一轮，返回成功入队的任务数。
// 单个工厂失败不阻塞其他工厂：错误累积返回，已生成任务照常入队。
func (m *Manager) RunOnce(ctx context.Context, now time.Time) (int, error) {
	var (
		enqueued int
		errs     []error
	)
	for _, f := range m.registry.All() {
		tasks, err := f.Generate(ctx, now)
		if err != nil {
			errs = append(errs, fmt.Errorf("factory %s: %w", f.Name(), err))
			continue
		}
		for _, t := range tasks {
			if err := m.sink(ctx, t); err != nil {
				errs = append(errs, fmt.Errorf("factory %s enqueue: %w", f.Name(), err))
				continue
			}
			enqueued++
		}
	}
	return enqueued, errors.Join(errs...)
}

// Run 为每个工厂起一个周期驱动，直到 ctx 结束。工厂周期 <= 0 视为不启用（跳过）。
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, f := range m.registry.All() {
		interval := f.Interval()
		if interval <= 0 {
			continue
		}
		wg.Add(1)
		go func(f Factory, interval time.Duration) {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					// 单工厂一轮：失败只记录，不终止循环（下一周期重试）。
					if _, err := m.RunOnceFactory(ctx, f, m.clock()); err != nil {
						continue
					}
				}
			}
		}(f, interval)
	}
	wg.Wait()
}

// RunOnceFactory 驱动单个工厂一轮。
func (m *Manager) RunOnceFactory(ctx context.Context, f Factory, now time.Time) (int, error) {
	tasks, err := f.Generate(ctx, now)
	if err != nil {
		return 0, fmt.Errorf("factory %s: %w", f.Name(), err)
	}
	enqueued := 0
	var errs []error
	for _, t := range tasks {
		if err := m.sink(ctx, t); err != nil {
			errs = append(errs, err)
			continue
		}
		enqueued++
	}
	return enqueued, errors.Join(errs...)
}
