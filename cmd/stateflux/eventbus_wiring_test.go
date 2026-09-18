package stateflux

import (
	"context"
	"testing"

	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// 已知逻辑 channel 名按约定形态解析；未登记的名字是 task 行记录的异步队列名，
// 一律按默认形态（Redis list）。
func TestQueueRegistrySpec(t *testing.T) {
	reg := newQueueRegistry(nil, channelNames())

	cases := []struct {
		name string
		kind channel.Kind
	}{
		{ChannelDefault, channel.KindRedisList},
		{ChannelSync, channel.KindRPCUnary},
		{ChannelStream, channel.KindRPCStream},
		{"redis-pubsub", channel.KindRedisPubSub},
		{"memory", channel.KindMemory},
		{"q1", channel.KindRedisList}, // 未登记 → 默认异步队列形态
	}
	for _, tc := range cases {
		spec, ok := reg.Spec(context.Background(), tc.name)
		if !ok {
			t.Fatalf("Spec(%q) not found", tc.name)
		}
		if spec.Name != tc.name || spec.Kind != tc.kind {
			t.Fatalf("Spec(%q) = %+v, want kind %s", tc.name, spec, tc.kind)
		}
	}
	if _, ok := reg.Spec(context.Background(), ""); ok {
		t.Fatal("empty name must not resolve")
	}

	def, ok := reg.Default(context.Background())
	if !ok || def.Name != ChannelDefault || def.Kind != channel.KindRedisList {
		t.Fatalf("Default() = %+v, %v; want %s/redis-list", def, ok, ChannelDefault)
	}
}

// 节点 → 队列映射来自队列路由反查；同一节点多个队列时取字典序最小者以保证可复现。
func TestQueueRegistryQueueForNode(t *testing.T) {
	view := cacheview.NewMem(cacheview.Options{})
	ctx := context.Background()
	if err := view.SetQueueRoute(ctx, "q2", "n1"); err != nil {
		t.Fatalf("set route q2: %v", err)
	}
	if err := view.SetQueueRoute(ctx, "q1", "n1"); err != nil {
		t.Fatalf("set route q1: %v", err)
	}
	if err := view.SetQueueRoute(ctx, "q3", "n2"); err != nil {
		t.Fatalf("set route q3: %v", err)
	}

	reg := newQueueRegistry(view, channelNames())
	name, ok := reg.QueueForNode(ctx, "n1")
	if !ok || name != "q1" {
		t.Fatalf("QueueForNode(n1) = %q, %v; want q1", name, ok)
	}
	if name, ok := reg.QueueForNode(ctx, "n3"); ok {
		t.Fatalf("QueueForNode(n3) = %q, true; want no mapping", name)
	}
	if _, ok := reg.QueueForNode(ctx, ""); ok {
		t.Fatal("empty node id must not resolve")
	}

	// 无路由来源（单机装配）时不做节点寻址。
	if _, ok := newQueueRegistry(nil, channelNames()).QueueForNode(ctx, "n1"); ok {
		t.Fatal("nil routes must not resolve a node")
	}
}

// 工厂按 Kind 分派：内存形态可直接创建；未知形态显式报错；Redis 形态缺客户端时报错而非 panic。
func TestChannelFactoryOpen(t *testing.T) {
	f := &channelFactory{}
	ctx := context.Background()

	ch, err := f.Open(ctx, eventbus.ChannelSpec{Name: "memory", Kind: channel.KindMemory})
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	if ch.Kind() != channel.KindMemory {
		t.Fatalf("kind = %s, want memory", ch.Kind())
	}
	if _, err := f.Open(ctx, eventbus.ChannelSpec{Name: "x", Kind: channel.Kind("bogus")}); err == nil {
		t.Fatal("unsupported kind must fail")
	}
	if _, err := f.Open(ctx, eventbus.ChannelSpec{Name: "q1", Kind: channel.KindRedisList}); err == nil {
		t.Fatal("redis kind without client must fail")
	}
}
