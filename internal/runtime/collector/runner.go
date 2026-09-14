package collector

import (
	"context"
	"errors"
	"sync"

	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/obs"
)

// Runner 把 Collector 接到某个 EventBus channel 的 result topic 上（§5.5）：
// 异步 Redis 结果通道或全双工 stream 的订阅侧都经它归集。默认部署下 worker 走
// gRPC ResultStream，由 biz.ExecutorServer.ResultStream 直接调用同一 Collector，
// 因此 Runner 主要服务于可替换的 Redis 结果通道。
type Runner struct {
	bus         *eventbus.EventBus
	channelName string
	topic       channel.Topic
	collector   *Collector
	metrics     *obs.Metrics

	mu  sync.Mutex
	sub channel.Subscription
}

// RunnerOptions 是 Runner 装配参数。
type RunnerOptions struct {
	Bus         *eventbus.EventBus
	ChannelName string
	Topic       channel.Topic
	Collector   *Collector
	Metrics     *obs.Metrics
}

// NewRunner 构造 Runner；ChannelName 为空时不订阅。
func NewRunner(opts RunnerOptions) *Runner {
	topic := opts.Topic
	if topic == "" {
		topic = eventbus.ResultTopic()
	}
	return &Runner{
		bus:         opts.Bus,
		channelName: opts.ChannelName,
		topic:       topic,
		collector:   opts.Collector,
		metrics:     opts.Metrics,
	}
}

// Start 建立订阅。
func (r *Runner) Start(ctx context.Context) error {
	if r.bus == nil || r.collector == nil {
		return errors.New("runtime/collector: runner requires eventbus and collector")
	}
	if r.channelName == "" {
		return nil
	}
	ch, err := r.bus.Resolve(r.channelName)
	if err != nil {
		return err
	}
	subber, ok := ch.(channel.Subscriber)
	if !ok {
		return errors.New("runtime/collector: result channel does not support subscribe")
	}
	sub, err := subber.Subscribe(ctx, r.topic, r.handle)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.sub = sub
	r.mu.Unlock()
	return nil
}

func (r *Runner) handle(ctx context.Context, env channel.Envelope) error {
	ev, err := decodeResult(env.Payload)
	if err != nil {
		return err
	}
	return r.collector.Consume(ctx, ev)
}

// Stop 关闭订阅（幂等）。
func (r *Runner) Stop() error {
	r.mu.Lock()
	sub := r.sub
	r.sub = nil
	r.mu.Unlock()
	if sub != nil {
		return sub.Close()
	}
	return nil
}
