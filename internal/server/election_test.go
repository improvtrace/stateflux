package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/internal/cluster"
)

type mutableView struct {
	mu   sync.Mutex
	info cluster.Info
}

func (v *mutableView) Get(context.Context) (cluster.Info, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.info, nil
}

func (v *mutableView) setScheduler(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.info.SchedulerNodeID = id
}

type countingComponent struct {
	mu     sync.Mutex
	starts int
	stops  int
}

func (c *countingComponent) Start(context.Context) error {
	c.mu.Lock()
	c.starts++
	c.mu.Unlock()
	return nil
}

func (c *countingComponent) Stop() error {
	c.mu.Lock()
	c.stops++
	c.mu.Unlock()
	return nil
}

func (c *countingComponent) snapshot() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts, c.stops
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout")
}

func TestElectionGateFollowsScheduler(t *testing.T) {
	view := &mutableView{}
	view.info = cluster.Info{
		Nodes:           []cluster.Node{{ID: "self"}, {ID: "other"}},
		SchedulerNodeID: "other",
	}
	ctx := context.Background()
	cache := cluster.NewCache(ctx, view, 0)
	defer cache.Close()

	inner := &countingComponent{}
	gate := newElectionGate("scheduler", cache, "self", inner, 20*time.Millisecond, nil)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := gate.Start(runCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = gate.Stop() }()

	time.Sleep(80 * time.Millisecond)
	if starts, _ := inner.snapshot(); starts != 0 {
		t.Fatalf("non-scheduler must not run inner, starts = %d", starts)
	}

	// 选举切换到本实例：inner 启动。
	view.setScheduler("self")
	if err := cache.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	waitUntil(t, func() bool { s, _ := inner.snapshot(); return s == 1 })

	// 选举切走：inner 停止。
	view.setScheduler("other")
	_ = cache.Refresh(ctx)
	waitUntil(t, func() bool { _, stops := inner.snapshot(); return stops == 1 })
}
