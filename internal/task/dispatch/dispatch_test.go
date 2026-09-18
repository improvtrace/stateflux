package dispatch

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/mem"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/codec"
)

// failingChannel 模拟通道发送失败，用于验证 at_most_once 不重试、at_least_once 返回错误。
type failingChannel struct{}

func (failingChannel) Kind() channel.Kind { return channel.KindRedisList }
func (failingChannel) Capabilities() channel.Capabilities {
	return channel.Capabilities{Subscribe: true}
}
func (failingChannel) Call(context.Context, channel.Envelope) (channel.Envelope, error) {
	return channel.Envelope{}, errors.New("boom")
}

func newDispatcher(t *testing.T, ch channel.Channel, cfg config.Dispatch) (*Dispatcher, cacheview.View) {
	t.Helper()
	bus, err := eventbus.NewStatic(map[string]channel.Channel{"q": ch}, "q")
	if err != nil {
		t.Fatalf("eventbus: %v", err)
	}
	reg := task.NewRegistry()
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err != nil {
		t.Fatalf("register: %v", err)
	}
	view := cacheview.NewMem(cacheview.Options{})
	d := New(Options{
		Bus:    bus,
		View:   view,
		Codec:  codec.New(reg),
		Self:   "n1",
		Config: cfg,
	})
	return d, view
}

func TestDeliverAsyncEncodesAndMarksView(t *testing.T) {
	memCh := mem.NewMemory()
	d, view := newDispatcher(t, memCh, config.Dispatch{
		DefaultSemantics: config.AtLeastOnce,
		DefaultDelivery:  config.DeliveryRedisQueue,
	})
	received := make(chan channel.Envelope, 1)
	sub, err := memCh.Subscribe(context.Background(), channel.Topic("q"), func(_ context.Context, env channel.Envelope) error {
		received <- env
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	msg := &taskv1.TaskMessage{TaskId: 7, Attempt: 1, Type: "demo", Operator: "beat", Channel: "q"}
	res, err := d.Deliver(context.Background(), Request{Message: msg})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !res.Accepted || res.TargetNodeID != "n1" {
		t.Fatalf("result = %+v", res)
	}
	env := <-received
	if env.Attempt != 1 || env.Key != "7" {
		t.Fatalf("envelope = %+v", env)
	}
	// 异步载荷是 codec 签名帧，消费端可重建 TaskMessage（§15.1#13）。
	reg := task.NewRegistry()
	_, _ = reg.Register(func() proto.Message { return &taskv1.TaskMessage{} })
	decoded, err := codec.New(reg).Decode(env.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.(*taskv1.TaskMessage).GetTaskId() != 7 {
		t.Fatalf("decoded = %+v", decoded)
	}
	st, ok, _ := view.GetTaskState(context.Background(), 7)
	if !ok || st.State != cacheview.StateDispatched {
		t.Fatalf("view state = %+v ok=%v", st, ok)
	}
}

func TestExactlyOnceDedupe(t *testing.T) {
	memCh := mem.NewMemory()
	d, _ := newDispatcher(t, memCh, config.Dispatch{
		DefaultSemantics: config.ExactlyOnce,
		DefaultDelivery:  config.DeliveryRedisQueue,
	})
	msg := &taskv1.TaskMessage{TaskId: 9, Attempt: 1, IdempotencyKey: "k-9", Channel: "q"}
	first, err := d.Deliver(context.Background(), Request{Message: msg})
	if err != nil || !first.Accepted || first.Duplicate {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	second, err := d.Deliver(context.Background(), Request{Message: msg})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.Duplicate {
		t.Fatalf("second should be duplicate: %+v", second)
	}
}

func TestSemanticsOnSendFailure(t *testing.T) {
	dAtMost, _ := newDispatcher(t, failingChannel{}, config.Dispatch{
		DefaultSemantics: config.AtMostOnce,
		DefaultDelivery:  config.DeliveryRedisQueue,
	})
	res, err := dAtMost.Deliver(context.Background(), Request{Message: &taskv1.TaskMessage{TaskId: 1, Channel: "q"}})
	if err != nil {
		t.Fatalf("at_most_once must swallow send failure: %v", err)
	}
	if res.Accepted || res.Retryable {
		t.Fatalf("at_most_once result = %+v", res)
	}

	dAtLeast, _ := newDispatcher(t, failingChannel{}, config.Dispatch{
		DefaultSemantics: config.AtLeastOnce,
		DefaultDelivery:  config.DeliveryRedisQueue,
	})
	if _, err := dAtLeast.Deliver(context.Background(), Request{Message: &taskv1.TaskMessage{TaskId: 2, Channel: "q"}}); err == nil {
		t.Fatal("at_least_once must surface send failure for retry")
	}
}
