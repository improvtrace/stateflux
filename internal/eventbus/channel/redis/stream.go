package redis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// Stream 是基于 Redis Stream 的异步单向队列（§3.2、§12.3）：Call = XADD，
// Subscribe = XREADGROUP（消费组）。XACK 只在实现内部执行以推进消费组游标，
// **不暴露为 domain 语义、也不构成可靠性条件**（§9.3）；stream 丢失仍由 R1 兜底。
type Stream struct{ base }

// NewStream 构造 stream 通道。
func NewStream(client redis.UniversalClient, opts Options) (*Stream, error) {
	b, err := NewBase(client, opts)
	if err != nil {
		return nil, err
	}
	return &Stream{base: b}, nil
}

// Kind 实现 channel.Channel。
func (*Stream) Kind() channel.Kind { return channel.KindRedisStream }

// Call 追加消息（XADD）。
func (s *Stream) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	raw, err := encodeEnvelope(env)
	if err != nil {
		return channel.Envelope{}, err
	}
	args := &redis.XAddArgs{
		Stream: s.opts.key(env.Topic),
		Values: map[string]any{"e": raw},
	}
	if s.opts.StreamMaxLen > 0 {
		args.MaxLen = s.opts.StreamMaxLen
		args.Approx = true
	}
	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return channel.Envelope{}, err
	}
	return simplex()
}

// Subscribe 以消费组读取并回调。
func (s *Stream) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	if h == nil {
		return nil, errNilHandler
	}
	key := s.opts.key(topic)
	// 消费组不存在则创建（BUSYGROUP 表示已存在，忽略）。
	if err := s.client.XGroupCreateMkStream(ctx, key, s.opts.Group, "0").Err(); err != nil &&
		!isBusyGroup(err) {
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if subCtx.Err() != nil {
				return
			}
			res, err := s.client.XReadGroup(subCtx, &redis.XReadGroupArgs{
				Group:    s.opts.Group,
				Consumer: s.opts.Consumer,
				Streams:  []string{key, ">"},
				Count:    16,
				Block:    s.opts.BlockTimeout,
			}).Result()
			if err != nil {
				if ctxErr(err) != nil && subCtx.Err() == nil {
					continue
				}
				if subCtx.Err() != nil {
					return
				}
				continue
			}
			for _, stream := range res {
				for _, msg := range stream.Messages {
					if raw, ok := msg.Values["e"].(string); ok {
						if env, derr := decodeEnvelope([]byte(raw), topic); derr == nil {
							_ = h(subCtx, env)
						}
					}
					// 实现级流控：确认已处理，避免 PEL 无界增长；不是可靠性条件。
					_ = s.client.XAck(subCtx, key, s.opts.Group, msg.ID).Err()
				}
			}
		}
	}()
	return newSubscription(cancel, done), nil
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= 9 && contains(err.Error(), "BUSYGROUP")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
