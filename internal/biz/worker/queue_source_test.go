package worker

import (
	"context"
	"sort"
	"testing"

	workerv1 "github.com/improvtrace/stateflux/api/stateflux/worker/v1"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	workerrt "github.com/improvtrace/stateflux/internal/worker"
)

func TestQueueSourceFallbackWithoutView(t *testing.T) {
	s := NewQueueSource(nil, []string{"default"})
	queues, err := s.AssignedQueues(context.Background(), "n1")
	if err != nil {
		t.Fatalf("assigned queues: %v", err)
	}
	if len(queues) != 1 || queues[0] != "default" {
		t.Fatalf("queues = %v", queues)
	}
}

func TestQueueSourceFallsBackWhenRoutesEmpty(t *testing.T) {
	view := cacheview.NewMem(cacheview.Options{})
	s := NewQueueSource(view, []string{"default"})

	// 视图尚未收到共识快照：回落静态配置，保证启动窗口仍能消费。
	queues, err := s.AssignedQueues(context.Background(), "n1")
	if err != nil {
		t.Fatalf("assigned queues: %v", err)
	}
	if len(queues) != 1 || queues[0] != "default" {
		t.Fatalf("queues = %v", queues)
	}
}

func TestQueueSourceFollowsRoutes(t *testing.T) {
	ctx := context.Background()
	view := cacheview.NewMem(cacheview.Options{})
	if err := view.SetQueueRoute(ctx, "q-b", "n1"); err != nil {
		t.Fatalf("set route: %v", err)
	}
	if err := view.SetQueueRoute(ctx, "q-a", "n1"); err != nil {
		t.Fatalf("set route: %v", err)
	}
	if err := view.SetQueueRoute(ctx, "q-other", "n2"); err != nil {
		t.Fatalf("set route: %v", err)
	}
	s := NewQueueSource(view, []string{"default"})

	queues, err := s.AssignedQueues(ctx, "n1")
	if err != nil {
		t.Fatalf("assigned queues: %v", err)
	}
	if !sort.StringsAreSorted(queues) {
		t.Fatalf("queues not sorted: %v", queues)
	}
	if len(queues) != 2 || queues[0] != "q-a" || queues[1] != "q-b" {
		t.Fatalf("queues = %v", queues)
	}
}

// ---- 能力服务端 ----

type stubVerifier struct{ called bool }

func (s *stubVerifier) Verify(context.Context, VerifyRequest) (VerifyResult, error) {
	s.called = true
	return VerifyResult{OK: true, Message: "ok"}, nil
}

type stubUploader struct {
	written int64
}

func (s *stubUploader) Upload(context.Context, UploadRequest) (int64, error) {
	return 42, nil
}

func TestCapabilityServerListAndInvoke(t *testing.T) {
	reg := workerrt.NewRegistry()
	verifier := &stubVerifier{}
	if err := RegisterCapabilities(reg, verifier, &stubUploader{}); err != nil {
		t.Fatalf("register capabilities: %v", err)
	}
	srv := NewCapabilityServer(reg, nil)

	list, err := srv.ListCapabilities(context.Background(), &workerv1.ListCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetCapabilities()) != 2 {
		t.Fatalf("capabilities = %d", len(list.GetCapabilities()))
	}

	resp, err := srv.VerifyPassword(context.Background(), &workerv1.VerifyPasswordRequest{
		Host: "h", Username: "u", Password: "p",
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !resp.GetOk() || !verifier.called {
		t.Fatalf("response = %+v, verifier called = %v", resp, verifier.called)
	}
}

func TestCapabilityServerUnknownCapability(t *testing.T) {
	srv := NewCapabilityServer(workerrt.NewRegistry(), nil)
	if _, err := srv.Invoke(context.Background(), &workerv1.InvokeRequest{Name: "nope"}); err == nil {
		t.Fatal("unknown capability must fail")
	}
}
