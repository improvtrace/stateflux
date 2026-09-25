package task

import (
	"context"
	"strings"
	"testing"

	"github.com/improvtrace/stateflux/internal/domain/repository"
	domaintask "github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/pkg/idgen"
)

// fakeStore 只覆写 Enqueue；其余方法来自内嵌接口（不应被调用）。
type fakeStore struct {
	repository.Store

	req       repository.EnqueueRequest
	result    repository.EnqueueResult
	err       error
	callCount int
}

func (f *fakeStore) Enqueue(_ context.Context, req repository.EnqueueRequest) (repository.EnqueueResult, error) {
	f.callCount++
	f.req = req
	return f.result, f.err
}

func TestPriorityOfClamps(t *testing.T) {
	if got := priorityOf(0); got != 50 {
		t.Fatalf("unset priority = %d, want default 50", got)
	}
	if got := priorityOf(-5); got != 0 {
		t.Fatalf("underflow = %d, want 0", got)
	}
	if got := priorityOf(250); got != 100 {
		t.Fatalf("overflow = %d, want 100", got)
	}
	if got := priorityOf(77); got != 77 {
		t.Fatalf("in-range = %d, want 77", got)
	}
}

func TestPayloadOrEmpty(t *testing.T) {
	if string(payloadOrEmpty(nil)) != "{}" {
		t.Fatal("empty payload must default to {}")
	}
	if got := string(payloadOrEmpty([]byte(`{"a":1}`))); got != `{"a":1}` {
		t.Fatalf("valid json = %s", got)
	}
	// 非 JSON 字节包成 JSON 字符串，保证 jsonb 列可写。
	got := payloadOrEmpty([]byte("plain-text"))
	if !strings.HasPrefix(string(got), `"plain-text`) {
		t.Fatalf("non-json payload = %s", got)
	}
}

func TestEnqueueBuildsRequest(t *testing.T) {
	store := &fakeStore{result: repository.EnqueueResult{TaskID: 99, Created: true}}
	e := NewEnqueuer(store, idgen.NewSnowflake("n1"), "default")

	id, created, err := e.Enqueue(context.Background(), domaintask.FuncTask{
		S: domaintask.Spec{
			Type:      "demo",
			Priority:  250, // 越界收敛
			TimeoutMS: 0,   // 取默认
		},
		P: []byte(`{"k":1}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id != 99 || !created {
		t.Fatalf("id=%d created=%v", id, created)
	}
	req := store.req
	if req.Type != "demo" || req.Channel != "default" || req.Priority != 100 {
		t.Fatalf("request = %+v", req)
	}
	if req.TimeoutMs != 60_000 || req.MaxAttempts != 3 {
		t.Fatalf("defaults = %+v", req)
	}
	if !strings.HasPrefix(req.IdempotencyKey, "auto:") {
		t.Fatalf("auto idempotency key = %q", req.IdempotencyKey)
	}
	if string(req.Payload) != `{"k":1}` {
		t.Fatalf("payload = %s", req.Payload)
	}
	if req.TaskID == 0 {
		t.Fatal("task id must come from snowflake")
	}
}

func TestEnqueueValidation(t *testing.T) {
	store := &fakeStore{}
	e := NewEnqueuer(store, idgen.NewSnowflake("n1"), "default")

	if _, _, err := e.Enqueue(context.Background(), domaintask.FuncTask{S: domaintask.Spec{}}); err == nil {
		t.Fatal("empty type must be rejected")
	}
	// defaultChannel 为空且 Spec 未给 channel 时报错。
	e2 := NewEnqueuer(store, idgen.NewSnowflake("n1"), "")
	if _, _, err := e2.Enqueue(context.Background(), domaintask.FuncTask{S: domaintask.Spec{Type: "t"}}); err == nil {
		t.Fatal("missing channel must be rejected")
	}
	// 非法回调规格在入账前被拦截。
	if _, _, err := e.Enqueue(context.Background(), domaintask.FuncTask{
		S: domaintask.Spec{Type: "t", Callback: []byte("{invalid")},
	}); err == nil {
		t.Fatal("invalid callback spec must be rejected")
	}
	if store.callCount != 0 {
		t.Fatalf("rejected enqueues must not hit the store: %d calls", store.callCount)
	}
}

func TestEnqueueDedupePassthrough(t *testing.T) {
	store := &fakeStore{result: repository.EnqueueResult{TaskID: 7, Created: false}}
	e := NewEnqueuer(store, idgen.NewSnowflake("n1"), "default")

	id, created, err := e.Enqueue(context.Background(), domaintask.FuncTask{S: domaintask.Spec{Type: "t", IdempotencyKey: "biz-key"}})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id != 7 || created {
		t.Fatalf("dedupe hit: id=%d created=%v", id, created)
	}
	if store.req.IdempotencyKey != "biz-key" {
		t.Fatalf("idempotency key = %q", store.req.IdempotencyKey)
	}
	if strings.HasPrefix(store.req.IdempotencyKey, "auto:") {
		t.Fatal("business key must be kept as-is")
	}
}
