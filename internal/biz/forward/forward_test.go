package forward

import (
	"context"
	"errors"
	"testing"

	forwardv1 "github.com/improvtrace/stateflux/api/stateflux/forward/v1"
)

type fakeRouter struct {
	called  bool
	target  string
	payload []byte
}

func (f *fakeRouter) Forward(_ context.Context, target, _ string, payload []byte, _ map[string]string, _ int32) ([]byte, error) {
	f.called = true
	f.target = target
	return payload, nil
}

func TestServerHandlesLocally(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("/svc/Method", func(_ context.Context, payload []byte, _ map[string]string) ([]byte, error) {
		return append([]byte("ok:"), payload...), nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	srv := NewServer(reg, nil, "self", 3)
	resp, err := srv.Forward(context.Background(), &forwardv1.ForwardRequest{
		Method: "/svc/Method", Payload: []byte("x"), TargetNodeId: "self",
	})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if string(resp.GetPayload()) != "ok:x" || resp.GetServedBy() != "self" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestServerRelaysToTarget(t *testing.T) {
	router := &fakeRouter{}
	srv := NewServer(NewRegistry(), router, "self", 3)
	resp, err := srv.Forward(context.Background(), &forwardv1.ForwardRequest{
		Method: "/svc/Method", Payload: []byte("p"), TargetNodeId: "peer", Ttl: 2,
	})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if !router.called || router.target != "peer" {
		t.Fatalf("router = %+v", router)
	}
	if resp.GetServedBy() != "peer" {
		t.Fatalf("served by = %q", resp.GetServedBy())
	}
}

func TestServerLoopAndTTLProtection(t *testing.T) {
	srv := NewServer(NewRegistry(), &fakeRouter{}, "self", 3)
	if _, err := srv.Forward(context.Background(), &forwardv1.ForwardRequest{
		Method: "/svc/Method", VisitedNodes: []string{"self"}, TargetNodeId: "peer", Ttl: 2,
	}); !errors.Is(err, ErrLoop) {
		t.Fatalf("expected loop error, got %v", err)
	}
	if _, err := srv.Forward(context.Background(), &forwardv1.ForwardRequest{
		Method: "/svc/Method", TargetNodeId: "peer", Ttl: 0,
	}); err == nil {
		t.Fatal("ttl exhausted must fail")
	}
	if _, err := srv.Forward(context.Background(), &forwardv1.ForwardRequest{
		Method: "/svc/Missing", TargetNodeId: "self",
	}); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("expected no handler error, got %v", err)
	}
}
