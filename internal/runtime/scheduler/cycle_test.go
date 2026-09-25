package scheduler

import (
	"context"
	"testing"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/dispatch"
)

// ---- 替身 ----

type fakeLedger struct {
	promoted     int
	promoteErr   error
	claimed      []repository.Claimed
	claimErr     error
	payloads     map[int64][]byte
	payloadErr   error
	deadLettered []int64
}

func (f *fakeLedger) Promote(context.Context, int) (int, error) {
	return f.promoted, f.promoteErr
}

func (f *fakeLedger) Claim(context.Context, int) ([]repository.Claimed, error) {
	return f.claimed, f.claimErr
}

func (f *fakeLedger) GetPayload(_ context.Context, taskID int64) ([]byte, error) {
	if f.payloadErr != nil {
		return nil, f.payloadErr
	}
	if f.payloads == nil {
		return nil, nil
	}
	return f.payloads[taskID], nil
}

func (f *fakeLedger) DeadLetter(_ context.Context, taskID int64) (bool, error) {
	f.deadLettered = append(f.deadLettered, taskID)
	return true, nil
}

type fakeDeliverer struct {
	err      error
	accepted bool
	requests []dispatch.Request
}

func (f *fakeDeliverer) Deliver(_ context.Context, req dispatch.Request) (*dispatch.Result, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	return &dispatch.Result{TaskID: req.Message.GetTaskId(), Accepted: f.accepted}, nil
}

func newTestCycle(ledger Ledger, d Deliverer) *LedgerCycle {
	return NewCycle(CycleOptions{
		Ledger:     ledger,
		Dispatcher: d,
		NodeID:     "self",
		Config: config.Dispatch{
			DefaultSemantics: task.AtLeastOnce,
			DefaultDelivery:  task.DeliveryRedisQueue,
		},
	})
}

// ---- 用例 ----

func TestCyclePromotesClaimsAndDispatches(t *testing.T) {
	ledger := &fakeLedger{
		promoted: 2,
		claimed: []repository.Claimed{
			{TaskID: 1, Type: "demo", Attempt: 1, MaxAttempts: 3, Channel: "default"},
		},
		payloads: map[int64][]byte{1: []byte(`{"k":1}`)},
	}
	d := &fakeDeliverer{accepted: true}
	c := newTestCycle(ledger, d)

	res, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.Promoted != 2 || res.Claimed != 1 || res.Dispatched != 1 || res.Skipped != 0 {
		t.Fatalf("result = %+v", res)
	}
	if len(d.requests) != 1 {
		t.Fatalf("deliver calls = %d", len(d.requests))
	}
	req := d.requests[0]
	if req.Queue != "default" {
		t.Fatalf("queue = %q", req.Queue)
	}
	if req.Message.GetTaskId() != 1 || req.Message.GetAttempt() != 1 || string(req.Message.GetPayload()) != `{"k":1}` {
		t.Fatalf("message = %+v", req.Message)
	}
	if req.Semantics != task.AtLeastOnce || req.Delivery != task.DeliveryRedisQueue {
		t.Fatalf("route = %s/%s", req.Semantics, req.Delivery)
	}
}

func TestCycleDeadLettersOverMaxAttempts(t *testing.T) {
	ledger := &fakeLedger{
		claimed: []repository.Claimed{{TaskID: 7, Attempt: 4, MaxAttempts: 3, Channel: "default"}},
	}
	d := &fakeDeliverer{accepted: true}
	c := newTestCycle(ledger, d)

	res, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.Skipped != 1 || res.Dispatched != 0 {
		t.Fatalf("result = %+v", res)
	}
	if len(ledger.deadLettered) != 1 || ledger.deadLettered[0] != 7 {
		t.Fatalf("dead lettered = %v", ledger.deadLettered)
	}
	if len(d.requests) != 0 {
		t.Fatal("over-limit task must not be dispatched")
	}
}

func TestCycleSkipsOnDeliverError(t *testing.T) {
	ledger := &fakeLedger{
		claimed:  []repository.Claimed{{TaskID: 1, Attempt: 1, MaxAttempts: 3, Channel: "default"}},
		payloads: map[int64][]byte{1: []byte("{}")},
	}
	d := &fakeDeliverer{err: context.DeadlineExceeded}
	c := newTestCycle(ledger, d)

	res, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("single dispatch failure must not fail the round: %v", err)
	}
	if res.Skipped != 1 || res.Dispatched != 0 {
		t.Fatalf("result = %+v", res)
	}
}

func TestCycleSkipsOnPayloadError(t *testing.T) {
	ledger := &fakeLedger{
		claimed:    []repository.Claimed{{TaskID: 1, Attempt: 1, MaxAttempts: 3, Channel: "default"}},
		payloadErr: context.Canceled,
	}
	d := &fakeDeliverer{accepted: true}
	c := newTestCycle(ledger, d)

	res, err := c.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.Skipped != 1 || res.Dispatched != 0 || len(d.requests) != 0 {
		t.Fatalf("result = %+v, deliver calls = %d", res, len(d.requests))
	}
}

func TestCycleRequiresLedgerAndDispatcher(t *testing.T) {
	c := NewCycle(CycleOptions{})
	if _, err := c.RunOnce(context.Background()); err == nil {
		t.Fatal("missing ledger/dispatcher must fail fast")
	}
}

// 编译期：替身满足消费侧接口。
var (
	_ Deliverer = (*fakeDeliverer)(nil)
	_ Ledger    = (*fakeLedger)(nil)
)
