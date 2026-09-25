package dispatch

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	dispatchv1 "github.com/improvtrace/stateflux/api/stateflux/dispatch/v1"
	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/task"
	taskdispatch "github.com/improvtrace/stateflux/internal/task/dispatch"
)

// ---- 替身 ----

type fakeDispatcher struct {
	results      []*taskdispatch.Result
	err          error
	delivered    []taskdispatch.Request
	resolveNode  cluster.Node
	resolveErr   error
	resolveCalls int
}

func (f *fakeDispatcher) Deliver(_ context.Context, req taskdispatch.Request) (*taskdispatch.Result, error) {
	f.delivered = append(f.delivered, req)
	if f.err != nil {
		return nil, f.err
	}
	res := &taskdispatch.Result{TaskID: req.Message.GetTaskId(), Accepted: true}
	if len(f.results) > 0 {
		res = f.results[0]
	}
	return res, nil
}

func (f *fakeDispatcher) ResolveTarget(*taskv1.TaskMessage) (cluster.Node, error) {
	f.resolveCalls++
	if f.resolveErr != nil {
		return cluster.Node{}, f.resolveErr
	}
	return f.resolveNode, nil
}

type fakeForwarder struct {
	err      error
	response *dispatchv1.DispatchResponse
	calls    int
}

func (f *fakeForwarder) Forward(context.Context, string, string, []byte, map[string]string, int32) ([]byte, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return proto.Marshal(f.response)
}

// ---- 枚举映射 ----

func TestSemanticsOfMapping(t *testing.T) {
	cases := map[dispatchv1.Semantics]task.Semantics{
		dispatchv1.Semantics_AT_LEAST_ONCE: task.AtLeastOnce,
		dispatchv1.Semantics_AT_MOST_ONCE:  task.AtMostOnce,
		dispatchv1.Semantics_EXACTLY_ONCE:  task.ExactlyOnce,
	}
	for in, want := range cases {
		if got := semanticsOf(in); got != want {
			t.Fatalf("semanticsOf(%v) = %v, want %v", in, got, want)
		}
	}
	// proto 零值即 at_least_once：未知取值同样收敛到该默认分支。
	if got := semanticsOf(dispatchv1.Semantics(99)); got != task.AtLeastOnce {
		t.Fatalf("semanticsOf(unknown) = %v", got)
	}
}

func TestDeliveryOfMapping(t *testing.T) {
	if got := deliveryOf(dispatchv1.Delivery_DELIVERY_SYNC_RPC); got != task.DeliverySyncRPC {
		t.Fatalf("deliveryOf sync = %v", got)
	}
	if got := deliveryOf(dispatchv1.Delivery_DELIVERY_ASYNC_REDIS); got != task.DeliveryRedisQueue {
		t.Fatalf("deliveryOf queue = %v", got)
	}
	// 未知形态回落异步队列。
	if got := deliveryOf(dispatchv1.Delivery(99)); got != task.DeliveryRedisQueue {
		t.Fatalf("deliveryOf unknown = %v", got)
	}
}

// ---- 转发判定 ----

func newTestServer(d taskDispatcher, f forwardClient, cfg config.Dispatch) *DispatchServer {
	return NewDispatchServer(DispatchServerOptions{
		Dispatcher: d,
		Forwarder:  f,
		Self:       "self",
		Config:     cfg,
	})
}

func TestShouldForwardMatrix(t *testing.T) {
	cfg := config.Dispatch{Forward: true}
	s := newTestServer(&fakeDispatcher{}, &fakeForwarder{}, cfg)

	if !s.shouldForward(&dispatchv1.DispatchRequest{AllowForward: true}, "n2") {
		t.Fatal("forwardable request to other node must forward")
	}
	if s.shouldForward(&dispatchv1.DispatchRequest{AllowForward: false}, "n2") {
		t.Fatal("request without allow_forward must not forward")
	}
	if s.shouldForward(&dispatchv1.DispatchRequest{AllowForward: true}, "self") {
		t.Fatal("target self must not forward")
	}

	noForward := config.Dispatch{Forward: false}
	if newTestServer(&fakeDispatcher{}, &fakeForwarder{}, noForward).shouldForward(&dispatchv1.DispatchRequest{AllowForward: true}, "n2") {
		t.Fatal("forward disabled by config must not forward")
	}
	if newTestServer(&fakeDispatcher{}, nil, cfg).shouldForward(&dispatchv1.DispatchRequest{AllowForward: true}, "n2") {
		t.Fatal("missing forwarder must not forward")
	}
}

func TestHandleForwardedForcesLocalDelivery(t *testing.T) {
	d := &fakeDispatcher{}
	f := &fakeForwarder{}
	s := newTestServer(d, f, config.Dispatch{Forward: true})

	// 转发到达本节点后即使允许转发也必须本地投递（环路保护，§15.3#4）。
	req := &dispatchv1.DispatchRequest{
		Task:         &taskv1.TaskMessage{TaskId: 5, Channel: "default"},
		TargetNodeId: "self",
		AllowForward: true,
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := s.HandleForwarded(context.Background(), payload, map[string]string{})
	if err != nil {
		t.Fatalf("handle forwarded: %v", err)
	}
	resp := &dispatchv1.DispatchResponse{}
	if err := proto.Unmarshal(out, resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.GetTaskId() != 5 || !resp.GetAccepted() {
		t.Fatalf("response = %+v", resp)
	}
	if len(d.delivered) != 1 {
		t.Fatalf("deliver calls = %d", len(d.delivered))
	}
	if f.calls != 0 {
		t.Fatal("forwarded request must not be forwarded again")
	}
}

func TestForwardDelegatesToForwarder(t *testing.T) {
	d := &fakeDispatcher{}
	f := &fakeForwarder{response: &dispatchv1.DispatchResponse{TaskId: 9, Accepted: true}}
	s := newTestServer(d, f, config.Dispatch{Forward: true})

	resp, err := s.Dispatch(context.Background(), &dispatchv1.DispatchRequest{
		Task:         &taskv1.TaskMessage{TaskId: 9, Channel: "default"},
		TargetNodeId: "n2",
		AllowForward: true,
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("forwarder calls = %d", f.calls)
	}
	if resp.GetForwardedBy() != "self" || !resp.GetAccepted() {
		t.Fatalf("response = %+v", resp)
	}
	if len(d.delivered) != 0 {
		t.Fatal("forwarded dispatch must not deliver locally")
	}
}

func TestDispatchRequiresTask(t *testing.T) {
	s := newTestServer(&fakeDispatcher{}, &fakeForwarder{}, config.Dispatch{Forward: true})
	if _, err := s.Dispatch(context.Background(), &dispatchv1.DispatchRequest{}); err == nil {
		t.Fatal("empty task must be rejected")
	}
}
