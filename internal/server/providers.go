package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/mem"
	redischan "github.com/improvtrace/stateflux/internal/eventbus/channel/redis"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
)

// 逻辑 channel 名（任务行 channel 列的取值域）：装配把名字映射到实现，换实现不改状态机。
const (
	// ChannelDefault 是默认异步队列（Redis list），对应 config.Dispatch.DefaultDelivery。
	ChannelDefault = "default"
	// ChannelSync 是同步 request/reply（unary RPC）。
	ChannelSync = "sync"
	// ChannelStream 是结果流/全双工（gRPC ResultStream）。
	ChannelStream = "stream"
)

// provideDialer 提供共享 gRPC 连接池。
func provideDialer() *rpc.Dialer { return rpc.NewDialer() }

// provideClusterView 按 config.Cluster 构造集群视图客户端（§15.1#3）。
func provideClusterView(cfg config.Config) (cluster.View, error) {
	return cluster.New(cfg.Cluster, cfg.Node)
}

// provideClusterCache 包装带快照的集群视图（热路径无网络 I/O）。
func provideClusterCache(ctx context.Context, cfg config.Config, view cluster.View) (*cluster.Cache, func()) {
	cache := cluster.NewCache(ctx, view, cfg.Cluster.PollInterval)
	return cache, func() { _ = cache.Close() }
}

// provideCacheView 构造任务异步执行视图（§15.1#9）：Redis 可用时走 Redis，否则退化为
// 进程内实现并在装配日志中显式声明。
func provideCacheView(cfg config.Config, d *data.Data) (cacheview.View, error) {
	opts := cacheview.Options{
		TaskTTL:   cfg.Dispatch.DedupeWindow,
		DedupeTTL: cfg.Dispatch.DedupeWindow,
		RouteTTL:  cfg.Coherence.RevisionTTL,
	}
	if d == nil || d.Redis() == nil {
		return cacheview.NewMem(opts), nil
	}
	return cacheview.NewRedis(d.Redis(), opts)
}

// provideEventBus 注册全部逻辑 channel（§3.2）：异步为 Redis 形态，同步为 unary RPC，
// 结果流为 gRPC stream；另提供内存 fake 以便单机冒烟与测试。
func provideEventBus(cfg config.Config, d *data.Data, dialer *rpc.Dialer) (*eventbus.EventBus, error) {
	if d == nil || d.Redis() == nil {
		return nil, errors.New("server: redis client is required for eventbus channels")
	}
	timeout := cfg.Dispatch.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ropts := redischan.Options{BlockTimeout: 2 * time.Second}

	list, err := redischan.NewList(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	zset, err := redischan.NewZSet(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	stream, err := redischan.NewStream(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	pubsub, err := redischan.NewPubSub(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}

	channels := map[string]channel.Channel{
		ChannelDefault: list,
		ChannelSync:    rpc.NewUnary(dialer, timeout),
		ChannelStream:  rpc.NewStream(dialer, timeout),
		"redis-list":   list,
		"redis-zset":   zset,
		"redis-stream": stream,
		"redis-pubsub": pubsub,
		"rpc-unary":    rpc.NewUnary(dialer, timeout),
		"rpc-stream":   rpc.NewStream(dialer, timeout),
		"memory":       mem.NewMemory(),
	}
	return eventbus.NewEventBus(channels)
}

// provideTimeoutContext 返回一个可取消的后台 context 供组件绑定生命周期。
func provideBackgroundContext() context.Context { return context.Background() }

// describeChannel 便于启动日志输出装配结果。
func describeChannel(bus *eventbus.EventBus, name string) string {
	ch, err := bus.Resolve(name)
	if err != nil {
		return fmt.Sprintf("%s=unregistered", name)
	}
	return fmt.Sprintf("%s=%s", name, ch.Kind())
}
