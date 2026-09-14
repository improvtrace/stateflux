package redis

import (
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// Options 是 Redis 通道的键空间与消费参数。
type Options struct {
	// Prefix 键前缀（如 stateflux:ch:）。
	Prefix string
	// BlockTimeout 阻塞读取的单次等待上限；<=0 用 DefaultBlockTimeout。
	BlockTimeout time.Duration
	// StreamMaxLen stream 的近似裁剪长度（XADD MAXLEN ~）；0 表示不裁剪。
	StreamMaxLen int64
	// Group 是 stream 消费组名；空用 DefaultGroup。
	Group string
	// Consumer 是 stream 消费者名；空用 DefaultConsumer。
	Consumer string
}

// DefaultBlockTimeout 是阻塞读取缺省等待上限。
const DefaultBlockTimeout = 2 * time.Second

// DefaultPrefix 是缺省键前缀。
const DefaultPrefix = "stateflux:ch:"

// DefaultGroup / DefaultConsumer 是 stream 缺省消费组与消费者。
const (
	DefaultGroup    = "stateflux"
	DefaultConsumer = "worker"
)

func (o Options) withDefaults() Options {
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}
	if o.BlockTimeout <= 0 {
		o.BlockTimeout = DefaultBlockTimeout
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = DefaultConsumer
	}
	return o
}

func (o Options) key(topic channel.Topic) string {
	// {ch} hashtag：让同一 topic 的键在 Redis Cluster 下落在同一槽。
	return o.Prefix + "{ch}:" + string(topic)
}

// base 承载全部 Redis 形态共享的 client 与选项；实现只声明能力差异。
type base struct {
	client redis.UniversalClient
	opts   Options
}

// NewBase 校验 client 并补齐选项。
func NewBase(client redis.UniversalClient, opts Options) (base, error) {
	if client == nil {
		return base{}, errors.New("eventbus/channel/redis: nil redis client")
	}
	return base{client: client, opts: opts.withDefaults()}, nil
}

// Capabilities：全部 Redis 形态都是异步单向投递——可订阅，不提供 request/reply；
// LocalAck 保持 false，因为框架不把任何 broker ack 提升为可靠性条件（§9.3）。
func (base) Capabilities() channel.Capabilities {
	return channel.Capabilities{Subscribe: true}
}

// simplex 返回单向发送的零值应答信封（「已尝试发送」，不承载业务语义）。
func simplex() (channel.Envelope, error) { return channel.Envelope{}, nil }

// ctxErr 把 redis.Nil 之外的基础错误透传，并把 context 取消规范化。
func ctxErr(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

// 编译期断言：全部形态满足 channel.Channel 与 channel.Subscriber。
var (
	_ channel.Channel    = (*List)(nil)
	_ channel.Subscriber = (*List)(nil)
	_ channel.Channel    = (*ZSet)(nil)
	_ channel.Subscriber = (*ZSet)(nil)
	_ channel.Channel    = (*Stream)(nil)
	_ channel.Subscriber = (*Stream)(nil)
	_ channel.Channel    = (*PubSub)(nil)
	_ channel.Subscriber = (*PubSub)(nil)
)
