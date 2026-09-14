package redis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// PubSub 是基于 Redis pub/sub 的异步广播（§3.2、§12.3）：Call = PUBLISH，
// Subscribe = SUBSCRIBE。广播语义意味着「没有订阅者时消息直接丢失」，
// 这只在 best-effort 契约内，收敛由 PG 对账完成（§6.1）。
type PubSub struct{ base }

// NewPubSub 构造 pub/sub 通道。
func NewPubSub(client redis.UniversalClient, opts Options) (*PubSub, error) {
	b, err := NewBase(client, opts)
	if err != nil {
		return nil, err
	}
	return &PubSub{base: b}, nil
}

// Kind 实现 channel.Channel。
func (*PubSub) Kind() channel.Kind { return channel.KindRedisPubSub }

// Call 广播（PUBLISH），返回单向确认。
func (p *PubSub) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	payload, err := encodeEnvelope(env)
	if err != nil {
		return channel.Envelope{}, err
	}
	if err := p.client.Publish(ctx, p.opts.key(env.Topic), payload).Err(); err != nil {
		return channel.Envelope{}, err
	}
	return simplex()
}

// Subscribe 订阅 topic 并持续回调。
func (p *PubSub) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	if h == nil {
		return nil, errNilHandler
	}
	ps := p.client.Subscribe(ctx, p.opts.key(topic))
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ps.Close()
		ch := ps.Channel(redis.WithChannelSize(64))
		for {
			select {
			case <-subCtx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				env, derr := decodeEnvelope([]byte(msg.Payload), topic)
				if derr != nil {
					continue
				}
				_ = h(subCtx, env)
			}
		}
	}()
	return newSubscription(cancel, done), nil
}
