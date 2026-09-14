package biz

import (
	"context"
	"testing"

	coherencev1 "github.com/improvtrace/stateflux/api/stateflux/coherence/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
)

func TestAllocatorDeterministic(t *testing.T) {
	nodes := []cluster.Node{
		{ID: "b", Roles: []string{"executor"}},
		{ID: "a", Roles: []string{"executor"}},
	}
	a := Allocator{SchedulerNodeID: "sched"}
	state := a.Assign([]string{"q2", "q1"}, nodes, 1, 0)
	if len(state.GetRoutes()) != 2 {
		t.Fatalf("routes = %+v", state.GetRoutes())
	}
	// 队列按名排序后轮转：q1 -> a，q2 -> b（节点按 ID 排序）。
	if state.Routes[0].GetQueue() != "q1" || state.Routes[0].GetNodeId() != "a" {
		t.Fatalf("route[0] = %+v", state.Routes[0])
	}
	if state.Routes[1].GetQueue() != "q2" || state.Routes[1].GetNodeId() != "b" {
		t.Fatalf("route[1] = %+v", state.Routes[1])
	}
}

func TestCoherenceStoreRevisionAndFilter(t *testing.T) {
	s := NewCoherenceStore()
	older := &coherencev1.CoherenceState{Revision: 2, Routes: []*coherencev1.QueueRoute{{Queue: "q", NodeId: "n1"}}}
	if !s.Apply(older) {
		t.Fatal("first apply should succeed")
	}
	if s.Apply(&coherencev1.CoherenceState{Revision: 1}) {
		t.Fatal("older revision must be rejected")
	}
	if s.Revision() != 2 {
		t.Fatalf("revision = %d", s.Revision())
	}
	forNode := s.ForNode("n1")
	if len(forNode.GetRoutes()) != 1 || forNode.GetRoutes()[0].GetQueue() != "q" {
		t.Fatalf("for node = %+v", forNode)
	}
	if node, ok := s.QueueFor("q"); !ok || node != "n1" {
		t.Fatalf("queue for = %q ok=%v", node, ok)
	}
	if _, ok := s.QueueFor("missing"); ok {
		t.Fatal("unknown queue must not resolve")
	}
}

func TestCoherenceServerNotifyMaterializesView(t *testing.T) {
	store := NewCoherenceStore()
	view := cacheview.NewMem(cacheview.Options{})
	srv := NewCoherenceServer(store, view, nil)
	state := &coherencev1.CoherenceState{Revision: 5, Routes: []*coherencev1.QueueRoute{{Queue: "default", NodeId: "n2"}}}
	resp, err := srv.Notify(context.Background(), &coherencev1.NotifyRequest{State: state, TargetNodeId: "n2"})
	if err != nil {
		t.Fatalf("notify: %v", err)
	}
	if resp.GetAppliedRevision() != 5 {
		t.Fatalf("revision = %d", resp.GetAppliedRevision())
	}
	if node, ok, _ := view.GetQueueRoute(context.Background(), "default"); !ok || node != "n2" {
		t.Fatalf("view route = %q ok=%v", node, ok)
	}
}
