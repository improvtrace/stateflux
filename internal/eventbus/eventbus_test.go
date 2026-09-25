package eventbus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// ---- 替身 ----

// fakeChannel 记录调用次数，并可选实现订阅能力。
type fakeChannel struct {
	calls int
	subs  int
}

func (c *fakeChannel) Call(context.Context, Envelope) (Envelope, error) {
	c.calls++
	return Envelope{}, nil
}
func (c *fakeChannel) Kind() channel.Kind { return channel.KindMemory }
func (c *fakeChannel) Capabilities() channel.Capabilities {
	return channel.Capabilities{Subscribe: true}
}
func (c *fakeChannel) Subscribe(context.Context, channel.Topic, Handler) (Subscription, error) {
	c.subs++
	return nopSubscription{}, nil
}

type nopSubscription struct{}

func (nopSubscription) Close() error { return nil }

// fakeRegistry 是 QueueRegistry 的固定映射替身。
type fakeRegistry struct {
	specs map[string]channel.Kind
	byMap map[string]string // nodeID → channel 名
}

func (r fakeRegistry) Spec(_ context.Context, name string) (ChannelSpec, bool) {
	kind, ok := r.specs[name]
	if !ok {
		return ChannelSpec{}, false
	}
	return ChannelSpec{Name: name, Kind: kind}, true
}

func (r fakeRegistry) QueueForNode(_ context.Context, nodeID string) (string, bool) {
	name, ok := r.byMap[nodeID]
	return name, ok
}

func (r fakeRegistry) Default(context.Context) (ChannelSpec, bool) {
	return ChannelSpec{Name: "default", Kind: channel.KindRedisList}, true
}

// fakeView 是 cluster.ClusterCacheView 的替身：只实现 EventBus 用到的 Node。
type fakeView struct{ nodes map[string]cluster.Node }

func (v fakeView) Node(id string) (cluster.Node, bool) {
	n, ok := v.nodes[id]
	return n, ok
}
func (v fakeView) Nodes() []cluster.Node           { return nil }
func (v fakeView) SchedulerNodeID() string         { return "sched" }
func (v fakeView) Snapshot() cluster.Info          { return cluster.Info{} }
func (v fakeView) Scheduler() (cluster.Node, bool) { return cluster.Node{}, false }
func (v fakeView) Select(string, string, string, int) (cluster.Node, bool) {
	return cluster.Node{}, false
}
func (v fakeView) Refresh(context.Context) error { return nil }
func (v fakeView) Close() error                  { return nil }

// countingFactory 记录每次创建的规格并返回同一形态的替身，便于断言「只创建一次」。
type countingFactory struct {
	opened []ChannelSpec
	ch     *fakeChannel
}

func (f *countingFactory) Open(_ context.Context, spec ChannelSpec) (channel.Channel, error) {
	f.opened = append(f.opened, spec)
	return f.ch, nil
}

// ---- 测试 ----

// 惰性创建：未预注册的 channel 在首次调用时创建，之后复用同一实例。
func TestLazyChannelCreatedOnceAndCached(t *testing.T) {
	factory := &countingFactory{ch: &fakeChannel{}}
	bus, err := NewEventBus(
		fakeView{},
		fakeRegistry{specs: map[string]channel.Kind{"q1": channel.KindRedisList}},
		factory,
		nil, "",
	)
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := bus.Call(ctx, Envelope{Topic: "q1"}, WithChannel("q1")); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if len(factory.opened) != 1 {
		t.Fatalf("factory opened %d times, want 1", len(factory.opened))
	}
	if factory.ch.calls != 2 {
		t.Fatalf("channel calls = %d, want 2", factory.ch.calls)
	}
}

// 节点寻址：target 非空时经 QueueForNode 解析队列名，并用节点地址补齐空 DSN。
func TestNodeAddressingFillsDSN(t *testing.T) {
	factory := &countingFactory{ch: &fakeChannel{}}
	bus, err := NewEventBus(
		fakeView{nodes: map[string]cluster.Node{"n1": {ID: "n1", Address: "10.0.0.1:9100"}}},
		fakeRegistry{
			specs: map[string]channel.Kind{"q1": channel.KindRPCUnary},
			byMap: map[string]string{"n1": "q1"},
		},
		factory,
		nil, "",
	)
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	if _, err := bus.Call(context.Background(), Envelope{Topic: "q1", Target: "n1"}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(factory.opened) != 1 {
		t.Fatalf("factory opened %d times, want 1", len(factory.opened))
	}
	if got := factory.opened[0]; got.Name != "q1" || got.Kind != channel.KindRPCUnary || got.DSN != "10.0.0.1:9100" {
		t.Fatalf("opened spec = %+v, want name=q1 kind=rpc-unary dsn=10.0.0.1:9100", got)
	}
}

// 节点不在集群视图中：报错而不是盲发。
func TestUnknownNodeFails(t *testing.T) {
	bus, err := NewEventBus(
		fakeView{},
		fakeRegistry{specs: map[string]channel.Kind{"q1": channel.KindRPCUnary}, byMap: map[string]string{"n1": "q1"}},
		&countingFactory{ch: &fakeChannel{}},
		nil, "",
	)
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	if _, err := bus.Call(context.Background(), Envelope{Topic: "q1", Target: "n1"}); err == nil {
		t.Fatal("want error for unknown node, got nil")
	}
}

// 静态装配：未预注册的 channel 不做惰性创建，直接报错。
func TestStaticBusRejectsUnregisteredChannel(t *testing.T) {
	bus, err := NewStatic(map[string]channel.Channel{"q1": &fakeChannel{}}, "q1")
	if err != nil {
		t.Fatalf("new static eventbus: %v", err)
	}
	if err := bus.Send(context.Background(), Envelope{Topic: "q2"}, WithChannel("q2")); err == nil {
		t.Fatal("want error for unregistered channel, got nil")
	} else if !strings.Contains(err.Error(), "not pre-registered") {
		t.Fatalf("error = %v, want it to mention not pre-registered", err)
	}
	// 预注册的名字仍可用：默认通道无需 options。
	if err := bus.Send(context.Background(), Envelope{Topic: "q1"}); err != nil {
		t.Fatalf("send on default channel: %v", err)
	}
}

// SubscribeTopic 走同一套解析：按 options 指定的 channel 订阅。
func TestSubscribeTopicUsesLazyChannel(t *testing.T) {
	factory := &countingFactory{ch: &fakeChannel{}}
	bus, err := NewEventBus(
		fakeView{},
		fakeRegistry{specs: map[string]channel.Kind{"q1": channel.KindRedisPubSub}},
		factory,
		nil, "",
	)
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	sub, err := bus.SubscribeTopic(context.Background(), channel.Topic("task.high"), func(context.Context, Envelope) error { return nil }, WithChannel("q1"))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
	if factory.ch.subs != 1 {
		t.Fatalf("subscriptions = %d, want 1", factory.ch.subs)
	}
}

// 关闭后拒绝创建新通道。
func TestClosedBusRejectsLazyCreation(t *testing.T) {
	factory := &countingFactory{ch: &fakeChannel{}}
	bus, err := NewEventBus(
		fakeView{},
		fakeRegistry{specs: map[string]channel.Kind{"q1": channel.KindRedisList}},
		factory,
		nil, "",
	)
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	err = bus.Send(context.Background(), Envelope{Topic: "q1"}, WithChannel("q1"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if len(factory.opened) != 0 {
		t.Fatalf("factory opened %d times after close, want 0", len(factory.opened))
	}
}
