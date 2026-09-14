// Package scheduler 是调度核心（§5.2/§5.3、§15.1#11）：约束晋升、原子认领、按任务 channel
// 与目标节点属性分发。按确认条款，调度运行时可以有**多个实例**，每个实例绑定一种触发方式
// （tick / notify / coherence / manual），互不影响地驱动同一套 Cycle。
//
// 随外部选举（ClusterView）启停；与 collector/reconcile 同属控制面（internal/runtime）。
package scheduler

import (
	"context"
	"sync/atomic"
	"time"
)

// Tick 是一次调度动作（由 Trigger 调用）。
type Tick func(ctx context.Context) error

// Trigger 是一种分发触发方式（§15.1#11）：它只决定「何时驱动一轮」，不包含调度语义。
type Trigger interface {
	// Name 是触发器名（实例标识，指标与诊断标签）。
	Name() string
	// Run 驱动 tick 直到 ctx 结束。
	Run(ctx context.Context, tick Tick) error
}

// ---- tick ----

// TickTrigger 按固定周期驱动（§10 claim batch / tick）。
type TickTrigger struct {
	Interval time.Duration
}

// Name 实现 Trigger。
func (TickTrigger) Name() string { return "tick" }

// Run 实现 Trigger。
func (t TickTrigger) Run(ctx context.Context, tick Tick) error {
	interval := t.Interval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
			if err := tick(ctx); err != nil {
				// 单轮失败不终止循环；错误由 Cycle 内部记录指标。
				continue
			}
		}
	}
}

// ---- manual ----

// ManualTrigger 由外部信号驱动（运维手动触发、测试）。
type ManualTrigger struct {
	// C 是触发通道；收到信号即驱动一轮。
	C <-chan struct{}
}

// Name 实现 Trigger。
func (ManualTrigger) Name() string { return "manual" }

// Run 实现 Trigger。
func (t ManualTrigger) Run(ctx context.Context, tick Tick) error {
	if t.C == nil {
		<-ctx.Done()
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-t.C:
			if !ok {
				return nil
			}
			if err := tick(ctx); err != nil {
				continue
			}
		}
	}
}

// ---- notify ----

// NotifyTrigger 由数据库/运维通知唤醒（§5.1：NOTIFY 只是唤醒优化，tick 不可关闭）。
// 它同时保留一个兜底周期，避免通知丢失导致调度停摆。
type NotifyTrigger struct {
	// Notifications 是外部通知源（如 PG LISTEN 通道）；nil 时退化为纯周期。
	Notifications <-chan struct{}
	// FallbackInterval 是通知缺失时的兜底周期。
	FallbackInterval time.Duration
}

// Name 实现 Trigger。
func (NotifyTrigger) Name() string { return "notify" }

// Run 实现 Trigger。
func (t NotifyTrigger) Run(ctx context.Context, tick Tick) error {
	fallback := t.FallbackInterval
	if fallback <= 0 {
		fallback = time.Second
	}
	tk := time.NewTicker(fallback)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
			_ = tick(ctx)
		case _, ok := <-t.Notifications:
			if !ok {
				t.Notifications = nil
				continue
			}
			_ = tick(ctx)
		}
	}
}

// ---- coherence ----

// RevisionSource 提供共识 revision（CoherenceStore 实现）。
type RevisionSource interface {
	Revision() int64
}

// CoherenceTrigger 在共识 revision 变化时驱动一轮（队列归属变化后重新分发，§15.1#4）。
// 它同时按 PollInterval 轮询 revision 并保留兜底周期。
type CoherenceTrigger struct {
	Source       RevisionSource
	PollInterval time.Duration
}

// Name 实现 Trigger。
func (CoherenceTrigger) Name() string { return "coherence" }

// Run 实现 Trigger。
func (t CoherenceTrigger) Run(ctx context.Context, tick Tick) error {
	interval := t.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	var last atomic.Int64
	if t.Source != nil {
		last.Store(t.Source.Revision())
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
			if t.Source == nil {
				_ = tick(ctx)
				continue
			}
			if rev := t.Source.Revision(); rev != last.Load() {
				last.Store(rev)
				_ = tick(ctx)
			}
		}
	}
}

// NamedTrigger 是给任意 Trigger 附加实例名的包装：用于同一种触发方式起多个实例
// （§15.1#11「多个实例」）。
type NamedTrigger struct {
	InstanceName string
	Inner        Trigger
}

// Name 实现 Trigger。
func (n NamedTrigger) Name() string {
	if n.InstanceName != "" {
		return n.InstanceName
	}
	return n.Inner.Name()
}

// Run 实现 Trigger。
func (n NamedTrigger) Run(ctx context.Context, tick Tick) error { return n.Inner.Run(ctx, tick) }
