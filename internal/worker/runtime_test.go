package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/mem"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/codec"
)

type fakeQueueSource struct {
	mu     sync.Mutex
	queues []string
}

func (f *fakeQueueSource) AssignedQueues(context.Context, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queues...), nil
}

func (f *fakeQueueSource) set(queues ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues = queues
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestRuntimeDynamicQueueSubscription(t *testing.T) {
	mem1, mem2 := mem.NewMemory(), mem.NewMemory()
	bus, err := eventbus.NewEventBus(map[string]channel.Channel{"q1": mem1, "q2": mem2})
	if err != nil {
		t.Fatalf("eventbus: %v", err)
	}
	reg := task.NewRegistry()
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err != nil {
		t.Fatalf("register: %v", err)
	}
	cdc := codec.New(reg)

	var handled atomic.Int64
	handlers := NewHandlerRegistry()
	if err := handlers.Register(HandlerFunc{
		T: "demo", O: "beat",
		F: func(context.Context, *taskv1.TaskMessage) (Result, error) {
			handled.Add(1)
			return Result{Outcome: taskv1.Outcome_OUTCOME_SUCCEEDED}, nil
		},
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	src := &fakeQueueSource{queues: []string{"q1"}}
	rt := NewRuntime(RuntimeOptions{
		Bus: bus, Codec: cdc, Handlers: handlers,
		Queues: []string{"q1", "q2"}, QueueSource: src,
		QueueRefresh: 50 * time.Millisecond, NodeID: "n1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = rt.Stop() }()

	waitFor(t, "initial subscription q1", func() bool {
		return len(rt.SubscribedQueues()) == 1 && rt.SubscribedQueues()[0] == "q1"
	})

	send := func(name string, id int64) {
		raw, err := cdc.Encode(&taskv1.TaskMessage{TaskId: id, Attempt: 1, Type: "demo", Operator: "beat", Channel: name})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		ch, _ := bus.Resolve(name)
		if _, err := ch.Call(ctx, channel.Envelope{Topic: channel.Topic(name), Key: "x", Payload: raw}); err != nil {
			t.Fatalf("send %s: %v", name, err)
		}
	}

	send("q1", 1)
	waitFor(t, "q1 handled", func() bool { return handled.Load() == 1 })

	// 共识映射变化：只消费 q2。
	src.set("q2")
	waitFor(t, "resubscribe to q2", func() bool {
		qs := rt.SubscribedQueues()
		return len(qs) == 1 && qs[0] == "q2"
	})
	send("q1", 2)
	time.Sleep(100 * time.Millisecond)
	if got := handled.Load(); got != 1 {
		t.Fatalf("q1 should be unsubscribed, handled = %d", got)
	}
	send("q2", 3)
	waitFor(t, "q2 handled", func() bool { return handled.Load() == 2 })
}

func TestRuntimeHighWaterPausesNewSubscriptions(t *testing.T) {
	mem1 := mem.NewMemory()
	bus, err := eventbus.NewEventBus(map[string]channel.Channel{"q1": mem1})
	if err != nil {
		t.Fatalf("eventbus: %v", err)
	}
	reg := task.NewRegistry()
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err != nil {
		t.Fatalf("register: %v", err)
	}
	wal := NewWAL(1)
	wal.Add(&taskv1.ResultEvent{TaskId: 1, Attempt: 1}) // 立即到达高水位
	if !wal.HighWater() {
		t.Fatal("wal should be at high water")
	}
	src := &fakeQueueSource{queues: []string{"q1"}}
	rt := NewRuntime(RuntimeOptions{
		Bus: bus, Codec: codec.New(reg), Handlers: NewHandlerRegistry(),
		Queues: []string{"q1"}, QueueSource: src,
		QueueRefresh: 30 * time.Millisecond, NodeID: "n1", WAL: wal,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = rt.Stop() }()

	time.Sleep(120 * time.Millisecond)
	if qs := rt.SubscribedQueues(); len(qs) != 0 {
		t.Fatalf("high water must pause new subscriptions, got %v", qs)
	}
	// 确认结果后解除高水位，下一轮刷新应恢复订阅。
	wal.Ack(1, 1)
	waitFor(t, "resume subscription after ack", func() bool { return len(rt.SubscribedQueues()) == 1 })
}
