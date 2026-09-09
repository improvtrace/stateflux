package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/sdk"
)

func newTestQueue(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb), mr
}

// TestRegisterAndReject 注册语义（§3.2/§6.2）：墓碑拒绝、同 attempt 拒绝、
// 更新 attempt 已注册时陈旧副本被拒、更小 attempt 已注册时新尝试接管。
func TestRegisterAndReject(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()

	ok, err := q.Register(ctx, 1, 1, "node-a", 30*time.Second)
	if err != nil || !ok.OK {
		t.Fatalf("register: %+v %v", ok, err)
	}

	// 同一 attempt 重复注册（重复投递）→ 拒绝。
	dup, err := q.Register(ctx, 1, 1, "node-b", 30*time.Second)
	if err != nil || dup.OK || dup.RejectOf != RejectAttempt {
		t.Fatalf("dup register: %+v %v", dup, err)
	}

	// 墓碑命中 → 拒绝（终态后旧副本）。
	if err := q.SetTombstones(ctx, []int64{2}, time.Minute); err != nil {
		t.Fatal(err)
	}
	tb, err := q.Register(ctx, 2, 1, "node-a", 30*time.Second)
	if err != nil || tb.OK || tb.RejectOf != RejectTombstone {
		t.Fatalf("tombstone register: %+v %v", tb, err)
	}

	// 更新 attempt 已注册 → 旧 attempt 陈旧副本被拒。
	if _, err := q.Register(ctx, 3, 2, "node-b", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	stale, err := q.Register(ctx, 3, 1, "node-a", 30*time.Second)
	if err != nil || stale.OK || stale.RejectOf != RejectAttempt {
		t.Fatalf("stale attempt register: %+v %v", stale, err)
	}

	// 更小 attempt 已注册 → 更大 attempt 接管（对账重置后重新认领）。
	takeover, err := q.Register(ctx, 3, 3, "node-c", 30*time.Second)
	if err != nil || !takeover.OK {
		t.Fatalf("takeover register: %+v %v", takeover, err)
	}
	entries, err := q.ListInprocess(ctx)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries: %+v %v", entries, err)
	}
	for _, e := range entries {
		if e.TaskID == 3 && e.Member.NodeID != "node-c" {
			t.Fatalf("takeover member: %+v", e.Member)
		}
	}
	_ = mr
}

// TestRenewOwnership 续约归属校验：僵尸节点无法续约新尝试的记录（§3.2）。
func TestRenewOwnership(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	if _, err := q.Register(ctx, 10, 1, "node-a", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	// 正确归属续约成功。
	ok, err := q.Renew(ctx, 10, 1, "node-a", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	// 僵尸节点（不同 node）续约被拒。
	zombie, err := q.Renew(ctx, 10, 1, "node-zombie", 30*time.Second)
	if err != nil || zombie {
		t.Fatalf("zombie renew: %v %v", zombie, err)
	}
	// 不同 attempt 续约被拒。
	wrongAttempt, err := q.Renew(ctx, 10, 2, "node-a", 30*time.Second)
	if err != nil || wrongAttempt {
		t.Fatalf("wrong attempt renew: %v %v", wrongAttempt, err)
	}
}

// TestRemoveOwnership 移除归属校验：僵尸节点的归集后移除被拒（§12.6）。
func TestRemoveOwnership(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	if _, err := q.Register(ctx, 11, 1, "node-a", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	// 僵尸节点移除被拒。
	if ok, err := q.Remove(ctx, 11, 1, "node-zombie"); err != nil || ok {
		t.Fatalf("zombie remove: %v %v", ok, err)
	}
	// 正确归属移除成功。
	if ok, err := q.Remove(ctx, 11, 1, "node-a"); err != nil || !ok {
		t.Fatalf("remove: %v %v", ok, err)
	}
	if n, _ := q.InprocessSize(ctx); n != 0 {
		t.Fatalf("size after remove: %d", n)
	}
	// ForceRemove 不校验归属（R2）。
	if _, err := q.Register(ctx, 12, 1, "node-a", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := q.ForceRemove(ctx, []int64{12}); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.InprocessSize(ctx); n != 0 {
		t.Fatalf("size after force remove: %d", n)
	}
}

// TestTombstoneTTL 墓碑 TTL 自动回收（§6.2）。
func TestTombstoneTTL(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()

	if err := q.SetTombstones(ctx, []int64{20}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, _ := q.HasTombstone(ctx, 20); !ok {
		t.Fatal("tombstone missing")
	}
	mr.FastForward(3 * time.Second)
	if ok, _ := q.HasTombstone(ctx, 20); ok {
		t.Fatal("tombstone must expire")
	}
}

// TestPushAndDepths 异步投递与队列深度（§3.2/§6.4）。
func TestPushAndDepths(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	msgs := map[sdk.Priority][][]byte{
		sdk.PriorityHigh:   {[]byte("h1"), []byte("h2")},
		sdk.PriorityNormal: {[]byte("n1")},
	}
	if err := q.Push(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	depths, err := q.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if depths[sdk.PriorityHigh] != 2 || depths[sdk.PriorityNormal] != 1 || depths[sdk.PriorityLow] != 0 {
		t.Fatalf("depths: %v", depths)
	}
}

// TestCapacityReport 节点容量上报（§3.2）。
func TestCapacityReport(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()

	if err := q.ReportCapacity(ctx, "node-a", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	free, ok, err := q.FreeSlots(ctx, "node-a")
	if err != nil || !ok || free != 5 {
		t.Fatalf("free slots: %d %v %v", free, ok, err)
	}
	mr.FastForward(2 * time.Minute)
	if _, ok, err := q.FreeSlots(ctx, "node-a"); err != nil || ok {
		t.Fatalf("expired capacity key must vanish: %v %v", ok, err)
	}
}
