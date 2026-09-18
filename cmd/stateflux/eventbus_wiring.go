package stateflux

import (
	"context"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/mem"
	redischan "github.com/improvtrace/stateflux/internal/eventbus/channel/redis"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
	"github.com/redis/go-redis/v9"
)

// channelFactory 按 ChannelSpec.Kind 分派到具体实现（eventbus.ChannelFactory）：EventBus
// 只声明「要什么形态」，拨号细节留在装配层（§3.2）。Redis 形态共享同一客户端，构造是
// 轻量包装；RPC 形态共享同一连接池 dialer。
type channelFactory struct {
	redis   redis.UniversalClient
	dialer  *rpc.Dialer
	timeout time.Duration
	ropts   redischan.Options
}

// Open 实现 eventbus.ChannelFactory。
func (f *channelFactory) Open(_ context.Context, spec eventbus.ChannelSpec) (channel.Channel, error) {
	switch spec.Kind {
	case channel.KindRedisList:
		return redischan.NewList(f.redis, f.ropts)
	case channel.KindRedisZSet:
		return redischan.NewZSet(f.redis, f.ropts)
	case channel.KindRedisStream:
		return redischan.NewStream(f.redis, f.ropts)
	case channel.KindRedisPubSub:
		return redischan.NewPubSub(f.redis, f.ropts)
	case channel.KindRPCUnary:
		return rpc.NewUnary(f.dialer, f.timeout), nil
	case channel.KindRPCStream:
		return rpc.NewStream(f.dialer, f.timeout), nil
	case channel.KindMemory:
		return mem.NewMemory(), nil
	default:
		return nil, fmt.Errorf("stateflux: unsupported channel kind %q for channel %q", spec.Kind, spec.Name)
	}
}

// queueRegistry 是 eventbus.QueueRegistry 的装配实现：
//
//   - Spec：逻辑 channel 名 → 形态。已知名字（default/sync/stream/redis-*/rpc-*）按其
//     约定形态；其余名字是 task 行记录的异步队列名，一律默认形态（Redis list）。
//   - QueueForNode：节点 → 队列，反查任务异步执行视图的队列路由（queue → node），
//     路由由共识同步写入、worker 侧读取，因此是动态的。
//   - Default：默认异步队列（ChannelDefault）。
type queueRegistry struct {
	routes cacheview.View
	kinds  map[string]channel.Kind
	def    eventbus.ChannelSpec
}

// newQueueRegistry 构造队列映射表；routes 为 nil 时节点寻址不可用（单机装配）。
func newQueueRegistry(routes cacheview.View, kinds map[string]channel.Kind) *queueRegistry {
	return &queueRegistry{
		routes: routes,
		kinds:  kinds,
		def:    eventbus.ChannelSpec{Name: ChannelDefault, Kind: channel.KindRedisList},
	}
}

// channelNames 返回已知逻辑 channel 名与其约定形态。
func channelNames() map[string]channel.Kind {
	return map[string]channel.Kind{
		ChannelDefault: channel.KindRedisList,
		ChannelSync:    channel.KindRPCUnary,
		ChannelStream:  channel.KindRPCStream,
		"redis-list":   channel.KindRedisList,
		"redis-zset":   channel.KindRedisZSet,
		"redis-stream": channel.KindRedisStream,
		"redis-pubsub": channel.KindRedisPubSub,
		"rpc-unary":    channel.KindRPCUnary,
		"rpc-stream":   channel.KindRPCStream,
		"memory":       channel.KindMemory,
	}
}

// Spec 实现 eventbus.QueueRegistry。
func (r *queueRegistry) Spec(_ context.Context, name string) (eventbus.ChannelSpec, bool) {
	if name == "" {
		return eventbus.ChannelSpec{}, false
	}
	kind, ok := r.kinds[name]
	if !ok {
		kind = r.def.Kind
	}
	return eventbus.ChannelSpec{Name: name, Kind: kind}, true
}

// QueueForNode 实现 eventbus.QueueRegistry：同一节点承载多个队列时取字典序最小者，
// 保证解析结果可复现；队列路由读失败时按「无映射」处理（调用侧报错，不猜测）。
func (r *queueRegistry) QueueForNode(ctx context.Context, nodeID string) (string, bool) {
	if r.routes == nil || nodeID == "" {
		return "", false
	}
	routes, err := r.routes.QueueRoutes(ctx)
	if err != nil {
		return "", false
	}
	best := ""
	for queue, node := range routes {
		if node != nodeID {
			continue
		}
		if best == "" || queue < best {
			best = queue
		}
	}
	return best, best != ""
}

// Default 实现 eventbus.QueueRegistry。
func (r *queueRegistry) Default(context.Context) (eventbus.ChannelSpec, bool) { return r.def, true }
