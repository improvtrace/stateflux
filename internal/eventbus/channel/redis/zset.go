package redis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// ZSet 是基于 Redis zset 的异步单向队列（§3.2、§12.3）：Call = ZADD（score 决定出队顺序），
// Subscribe = ZPOPMIN 轮询。score 缺省用纳秒时间戳（近似 FIFO），可由
// Headers["score"] 覆盖，用于把任务 priority/时间门槛映射为出队顺序。
//
// 注意：score 只是实现级投递提示，不替代 PG 的 priority 排序（§5.2）。
type ZSet struct{ base }

// NewZSet 构造 zset 通道。
func NewZSet(client redis.UniversalClient, opts Options) (*ZSet, error) {
	b, err := NewBase(client, opts)
	if err != nil {
		return nil, err
	}
	return &ZSet{base: b}, nil
}

// Kind 实现 channel.Channel。
func (*ZSet) Kind() channel.Kind { return channel.KindRedisZSet }

// Call 入队（ZADD）。
func (z *ZSet) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	payload, err := encodeEnvelope(env)
	if err != nil {
		return channel.Envelope{}, err
	}
	score := float64(time.Now().UnixNano())
	if raw := env.Headers["score"]; raw != "" {
		if v, perr := strconv.ParseFloat(raw, 64); perr == nil {
			score = v
		}
	}
	if err := z.client.ZAdd(ctx, z.opts.key(env.Topic), redis.Z{Score: score, Member: payload}).Err(); err != nil {
		return channel.Envelope{}, err
	}
	return simplex()
}

// Subscribe 轮询 ZPOPMIN 并回调。
func (z *ZSet) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	if h == nil {
		return nil, errNilHandler
	}
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		key := z.opts.key(topic)
		for {
			if subCtx.Err() != nil {
				return
			}
			res, err := z.client.ZPopMin(subCtx, key, 1).Result()
			if err != nil {
				if subCtx.Err() != nil {
					return
				}
				sleepCtx(subCtx, z.opts.BlockTimeout)
				continue
			}
			if len(res) == 0 {
				sleepCtx(subCtx, z.opts.BlockTimeout)
				continue
			}
			member, ok := res[0].Member.(string)
			if !ok {
				continue
			}
			env, derr := decodeEnvelope([]byte(member), topic)
			if derr != nil {
				continue
			}
			_ = h(subCtx, env)
		}
	}()
	return newSubscription(cancel, done), nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
