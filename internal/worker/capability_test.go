package worker

import (
	"context"
	"strings"
	"testing"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

func TestCapabilityRegistry(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Func{
		D: Descriptor{Name: "echo", Description: "echo"},
		F: func(_ context.Context, req Request) (Response, error) {
			return Response{Payload: req.Payload}, nil
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Register(Func{D: Descriptor{Name: "echo"}}); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	resp, err := reg.Invoke(context.Background(), Request{Name: "echo", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(resp.Payload) != "hi" {
		t.Fatalf("payload = %q", resp.Payload)
	}
	if _, err := reg.Invoke(context.Background(), Request{Name: "missing"}); err == nil {
		t.Fatal("unknown capability must fail")
	}
	if descs := reg.Descriptors(); len(descs) != 1 || descs[0].Name != "echo" {
		t.Fatalf("descriptors = %+v", descs)
	}
}

func TestCapabilityHandlerRegistry(t *testing.T) {
	reg := NewHandlerRegistry()
	if err := reg.Register(HandlerFunc{T: "demo", O: "beat"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, ok := reg.Resolve("demo", "beat"); !ok {
		t.Fatal("handler should resolve")
	}
	if _, ok := reg.Resolve("demo", "other"); ok {
		t.Fatal("unexpected handler")
	}
	if err := reg.Register(HandlerFunc{T: ""}); err == nil || !strings.Contains(err.Error(), "type") {
		t.Fatalf("empty type must fail: %v", err)
	}
}

func TestWALAddAckAndReplay(t *testing.T) {
	w := NewWAL(2)
	w.Add(&taskv1.ResultEvent{TaskId: 1, Attempt: 1})
	w.Add(&taskv1.ResultEvent{TaskId: 2, Attempt: 1})
	if w.Len() != 2 {
		t.Fatalf("len = %d, want 2", w.Len())
	}
	w.Ack(1, 1)
	if w.Len() != 1 {
		t.Fatalf("len after ack = %d, want 1", w.Len())
	}
	// 超出容量丢弃最旧条目（易失派生数据，§14.7）。
	w.Add(&taskv1.ResultEvent{TaskId: 3, Attempt: 1})
	if w.Len() != 2 {
		t.Fatalf("len = %d, want 2 (bounded)", w.Len())
	}
	pending := w.Pending()
	if len(pending) != 2 || pending[0].GetTaskId() != 2 || pending[1].GetTaskId() != 3 {
		t.Fatalf("pending = %+v", pending)
	}
}
