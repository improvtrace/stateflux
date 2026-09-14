package reconcile

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLedger struct{ resets atomic.Int64 }

func (f *fakeLedger) ResetExpired(context.Context, time.Time, int) (int, error) {
	f.resets.Add(1)
	return 0, nil
}

type fakeProbe struct{ available atomic.Bool }

func (p *fakeProbe) ChannelsAvailable(context.Context) (bool, error) {
	return p.available.Load(), nil
}

func waitFor(t *testing.T, cond func() bool) {
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

func TestR4ProbeTriggersImmediateScan(t *testing.T) {
	ledger := &fakeLedger{}
	probe := &fakeProbe{}
	probe.available.Store(false)
	r := New(Options{
		Ledger:        ledger,
		Interval:      time.Hour, // 常规 tick 不触发，只有 R4 探测会
		Probe:         probe,
		ProbeInterval: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = r.Stop() }()

	waitFor(t, func() bool { return ledger.resets.Load() >= 2 })
}

func TestR4ProbeSkipsWhenAvailable(t *testing.T) {
	ledger := &fakeLedger{}
	probe := &fakeProbe{}
	probe.available.Store(true)
	r := New(Options{
		Ledger:        ledger,
		Interval:      time.Hour,
		Probe:         probe,
		ProbeInterval: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = r.Stop() }()

	time.Sleep(150 * time.Millisecond)
	if got := ledger.resets.Load(); got != 0 {
		t.Fatalf("resets = %d, want 0 while channels available", got)
	}
}
