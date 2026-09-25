package cluster

import (
	"context"
	"testing"
)

func TestSelectFiltersByConstraints(t *testing.T) {
	nodes := []Node{
		{ID: "b", VPC: "v2", Labels: []string{"gpu"}},
		{ID: "a", VPC: "v1", Labels: []string{"cpu"}},
		{ID: "c", VPC: "v1", Labels: []string{"cpu"}},
	}
	if n, ok := Select(nodes, "", "v1", "cpu", 0); !ok || n.ID != "a" {
		t.Fatalf("select = %+v ok=%v; want stable first by ID", n, ok)
	}
	if _, ok := Select(nodes, "", "v9", "", 0); ok {
		t.Fatal("no matching vpc must return false")
	}
	if n, ok := Select(nodes, "c", "", "", 0); !ok || n.ID != "c" {
		t.Fatalf("explicit node = %+v ok=%v", n, ok)
	}
}

func TestSelectBucketIsStable(t *testing.T) {
	nodes := []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	first, ok := Select(nodes, "", "", "", 7)
	if !ok {
		t.Fatal("bucket select must resolve")
	}
	for range 10 {
		if n, _ := Select(nodes, "", "", "", 7); n.ID != first.ID {
			t.Fatalf("bucket not deterministic: %s vs %s", n.ID, first.ID)
		}
	}
	// bucket 超过候选数时按模收敛，仍可解析。
	if n, ok := Select(nodes, "", "", "", 999); !ok || n.ID == "" {
		t.Fatalf("overflow bucket = %+v ok=%v", n, ok)
	}
}

func TestPermissionsOf(t *testing.T) {
	primary := permissionsOf([]string{"primary"})
	if len(primary) != 4 {
		t.Fatalf("primary perms = %v", primary)
	}
	standby := permissionsOf([]string{"standby"})
	if len(standby) != 2 {
		t.Fatalf("standby perms = %v", standby)
	}
	for _, p := range standby {
		if p != NodePermissionOperation && p != NodePermissionExecute {
			t.Fatalf("standby must not hold write permission %q", p)
		}
	}
}

func TestInfoNodeAndScheduler(t *testing.T) {
	info := Info{
		Nodes:           []Node{{ID: "a"}, {ID: "b", IsLeader: true}},
		SchedulerNodeID: "b",
	}
	if n, ok := info.Node("a"); !ok || n.ID != "a" {
		t.Fatalf("node lookup = %+v", n)
	}
	if _, ok := info.Node("zz"); ok {
		t.Fatal("unknown node must not resolve")
	}
	if n, ok := info.Scheduler(); !ok || n.ID != "b" {
		t.Fatalf("scheduler = %+v ok=%v", n, ok)
	}
}

func TestBucketOfRange(t *testing.T) {
	if BucketOf("") != 0 {
		t.Fatal("empty key must map to 0 (unrestricted)")
	}
	for _, key := range []string{"user:1", "order:42", "x"} {
		if b := BucketOf(key); b < 0 || b > 255 {
			t.Fatalf("bucket %d out of range for %q", b, key)
		}
	}
}

type fakeView struct {
	info Info
}

func (v *fakeView) Get(context.Context) (Info, error) { return v.info, nil }

func TestCacheSnapshotAndSchedulerNodeID(t *testing.T) {
	view := &fakeView{info: Info{
		Nodes:           []Node{{ID: "n1", Address: "h1"}, {ID: "n2", Address: "h2"}},
		NodeID:          "n1",
		SchedulerNodeID: "n2",
	}}
	cache := NewCache(context.Background(), view, 0) // interval=0：仅按需拉取

	if got := cache.SchedulerNodeID(); got != "n2" {
		t.Fatalf("scheduler node id = %q", got)
	}
	if n, ok := cache.Node("n1"); !ok || n.Address != "h1" {
		t.Fatalf("node = %+v ok=%v", n, ok)
	}
	if got := cache.Snapshot().NodeID; got != "n1" {
		t.Fatalf("snapshot node id = %q", got)
	}

	// 视图更新后 Refresh 应反映新快照。
	view.info.SchedulerNodeID = "n1"
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := cache.SchedulerNodeID(); got != "n1" {
		t.Fatalf("after refresh scheduler = %q", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
