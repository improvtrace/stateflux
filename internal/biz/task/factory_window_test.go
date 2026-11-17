package task

import (
	"context"
	"testing"
	"time"
)

// 亚秒级生成周期不得触发窗口除零（曾用秒刻度：interval<1s 时除数为 0 直接 panic）。
func TestHeartbeatFactorySubSecondInterval(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, interval := range []time.Duration{200 * time.Millisecond, 1 * time.Second, 5 * time.Second} {
		f := NewHeartbeatFactory(interval, "default")
		tasks, err := f.Generate(context.Background(), now)
		if err != nil {
			t.Fatalf("interval=%s Generate(): %v", interval, err)
		}
		if len(tasks) != 1 {
			t.Fatalf("interval=%s Generate() = %d tasks, want 1", interval, len(tasks))
		}
	}

	// <1ms 的周期直接不生成（毫秒刻度的除数下界）。
	f := NewHeartbeatFactory(500*time.Microsecond, "default")
	tasks, err := f.Generate(context.Background(), now)
	if err != nil {
		t.Fatalf("sub-milli Generate(): %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("sub-milli Generate() = %d tasks, want 0", len(tasks))
	}
}

// 同一窗口内重复生成必须得到相同幂等键（窗口唯一性），跨窗口键不同。
func TestHeartbeatFactoryWindowIdempotency(t *testing.T) {
	interval := 2 * time.Second
	f := NewHeartbeatFactory(interval, "default")
	base := time.UnixMilli(1_000_000)
	a, _ := f.Generate(context.Background(), base)
	b, _ := f.Generate(context.Background(), base.Add(interval/2))
	c, _ := f.Generate(context.Background(), base.Add(interval*2))
	if a[0].Spec().IdempotencyKey != b[0].Spec().IdempotencyKey {
		t.Fatal("same window produced different idempotency keys")
	}
	if a[0].Spec().IdempotencyKey == c[0].Spec().IdempotencyKey {
		t.Fatal("different windows produced the same idempotency key")
	}
}
