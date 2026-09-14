package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// List 是基于 Redis list 的异步单向队列（§3.2、§12.3）：Call = LPUSH，Subscribe = BRPOP
// 轮询（FIFO）。它是异步任务分发的默认 Redis 形态。
//
// 可靠性纪律：Redis 丢失即消息丢失，由 PG 的 processing grace + R1 重投兜底（§6.1）；
// 本实现不提供「恢复队列」的动作，也不把 list 长度当唯一反压信号（§6.3）。
type List struct{ base }

// NewList 构造 list 通道。
func NewList(client redis.UniversalClient, opts Options) (*List, error) {
	b, err := NewBase(client, opts)
	if err != nil {
		return nil, err
	}
	return &List{base: b}, nil
}

// Kind 实现 channel.Channel。
func (*List) Kind() channel.Kind { return channel.KindRedisList }

// Call 入队（LPUSH），返回单向确认。
func (l *List) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	payload, err := encodeEnvelope(env)
	if err != nil {
		return channel.Envelope{}, err
	}
	if err := l.client.LPush(ctx, l.opts.key(env.Topic), payload).Err(); err != nil {
		return channel.Envelope{}, err
	}
	return simplex()
}

// Subscribe 持续 BRPOP 并在独立 goroutine 回调；Close/ctx 结束即停止。
func (l *List) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	if h == nil {
		return nil, errNilHandler
	}
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		key := l.opts.key(topic)
		for {
			if subCtx.Err() != nil {
				return
			}
			res, err := l.client.BRPop(subCtx, l.opts.BlockTimeout, key).Result()
			if err != nil {
				if subCtx.Err() != nil {
					return
				}
				if errors.Is(err, redis.Nil) {
					// 阻塞超时（队列为空）：继续等待，绝不能把空队列当成订阅结束。
					continue
				}
				// 连接类错误：短暂退避后重试，不视为需要重投。
				sleepCtx(subCtx, 100*time.Millisecond)
				continue
			}
			if len(res) < 2 {
				continue
			}
			env, derr := decodeEnvelope([]byte(res[1]), topic)
			if derr != nil {
				continue
			}
			_ = h(subCtx, env)
		}
	}()
	return newSubscription(cancel, done), nil
}
