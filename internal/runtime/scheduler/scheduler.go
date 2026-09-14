package scheduler

import (
	"context"
	"errors"
	"sync"

	"github.com/improvtrace/stateflux/internal/obs"
)

// CycleResult 是一轮调度的产出统计。
type CycleResult struct {
	Promoted   int
	Claimed    int
	Dispatched int
	Skipped    int
}

// Cycle 是一轮完整调度：约束晋升 → 原子认领 → 分发（§5.2/§5.3）。
// 具体实现见 cycle.go；接口在此以便触发器与实现解耦、便于测试替身。
type Cycle interface {
	RunOnce(ctx context.Context) (CycleResult, error)
}

// Scheduler 是一个调度运行时实例（§15.1#11）：一个 Trigger + 一个 Cycle。
// 同一进程可承载多个 Scheduler 实例，各自用不同触发方式驱动。
type Scheduler struct {
	name    string
	trigger Trigger
	cycle   Cycle
	metrics *obs.Metrics

	mu      sync.Mutex
	running bool
}

// Options 是 Scheduler 装配参数。
type Options struct {
	Name    string
	Trigger Trigger
	Cycle   Cycle
	Metrics *obs.Metrics
}

// New 构造调度实例。
func New(opts Options) *Scheduler {
	name := opts.Name
	if name == "" && opts.Trigger != nil {
		name = opts.Trigger.Name()
	}
	return &Scheduler{name: name, trigger: opts.Trigger, cycle: opts.Cycle, metrics: opts.Metrics}
}

// Name 返回实例名。
func (s *Scheduler) Name() string { return s.name }

// Run 驱动实例直到 ctx 结束；重复 Run 返回错误。
func (s *Scheduler) Run(ctx context.Context) error {
	if s.trigger == nil {
		return errors.New("runtime/scheduler: nil trigger")
	}
	if s.cycle == nil {
		return errors.New("runtime/scheduler: nil cycle")
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("runtime/scheduler: already running")
	}
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()
	return s.trigger.Run(ctx, s.tick)
}

// tick 是 Trigger 驱动的单轮：执行 Cycle 并把错误交给指标，不向触发器返回致命错误
// （单轮失败不应终止整个调度实例）。
func (s *Scheduler) tick(ctx context.Context) error {
	res, err := s.cycle.RunOnce(ctx)
	if err != nil && s.metrics != nil {
		s.metrics.ResetCount(ctx, "cycle_error", 1)
	}
	_ = res
	return err
}

// Group 托管一组调度实例并统一启停。
type Group struct {
	schedulers []*Scheduler
}

// NewGroup 构造实例组。
func NewGroup(schedulers ...*Scheduler) *Group { return &Group{schedulers: schedulers} }

// Run 并发运行全部实例直到 ctx 结束。
func (g *Group) Run(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		errs = make(chan error, len(g.schedulers))
	)
	for _, s := range g.schedulers {
		wg.Add(1)
		go func(s *Scheduler) {
			defer wg.Done()
			if err := s.Run(ctx); err != nil {
				errs <- err
			}
		}(s)
	}
	wg.Wait()
	close(errs)
	return errors.Join(collect(errs)...)
}

func collect(ch <-chan error) []error {
	var out []error
	for err := range ch {
		out = append(out, err)
	}
	return out
}
