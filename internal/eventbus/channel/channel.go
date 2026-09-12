package channel

import (
	"context"
	"errors"
)

// Kind 标识一个具体实现（§3.2、§14.1）。任务行记录的是逻辑 channel 名，运行期由装配把
// 名字映射到某个实现，因此换实现不改状态机。
type Kind string

const (
	// KindMemory 是 in-memory fake：所有运行时测试的通信基座（§12.1），不得用于生产。
	KindMemory Kind = "memory"
	// KindRPCUnary 是半双工 request/reply：一次发送对应一个结果，调用方阻塞等待（unary RPC）。
	KindRPCUnary Kind = "rpc-unary"
	// KindRPCStream 是全双工 gRPC 双向 stream：默认的结果归集通道（§3.2）。
	KindRPCStream Kind = "rpc-stream"
	// KindRedisList / KindRedisZSet / KindRedisStream / KindRedisPubSub 是 Redis 侧的
	// best-effort 实现：异步单向投递，可靠性不依赖 Redis 的任何持久化机制（§9.2、§9.3）。
	KindRedisList   Kind = "redis-list"
	KindRedisZSet   Kind = "redis-zset"
	KindRedisStream Kind = "redis-stream"
	KindRedisPubSub Kind = "redis-pubsub"
)

// Duplex 是双工形态（§14.1）：由能力声明推导，调度侧据此决定是否需要等待结果。
type Duplex uint8

const (
	// DuplexSimplex 单向：发送不带结果，结果由对端之后另行发布。
	DuplexSimplex Duplex = iota
	// DuplexHalf 半双工 request/reply：先发送请求、再读取一个结果。
	DuplexHalf
	// DuplexFull 全双工：两端可并发发送与订阅。
	DuplexFull
)

func (d Duplex) String() string {
	switch d {
	case DuplexHalf:
		return "half"
	case DuplexFull:
		return "full"
	default:
		return "simplex"
	}
}

// Capabilities 是实现的自我声明（§3.2）。LocalAck 只表示实现暴露了投递确认信号，
// 可用于流控与诊断，**永不构成任务可靠性条件**。
type Capabilities struct {
	Subscribe    bool
	RequestReply bool
	FullDuplex   bool
	LocalAck     bool
}

// Duplex 由能力声明推导双工形态。
func (c Capabilities) Duplex() Duplex {
	switch {
	case c.FullDuplex:
		return DuplexFull
	case c.RequestReply:
		return DuplexHalf
	default:
		return DuplexSimplex
	}
}

// Topic 是逻辑主题（§3.2）：任务为 task.{priority}，结果为 result。
type Topic string

// Envelope 是通道载荷的通用外形（§3.3）。业务语义由 api/stateflux/task/v1 的
// TaskMessage / ResultEvent 承担，实现只搬运字节与元数据，不解释也不修改内容。
type Envelope struct {
	Topic Topic
	// Key 是 task_id：用于去重、审计与分区，不作为幂等权威（权威是 PG 的 task_identities）。
	Key string
	// Attempt 是 attempt fence（§3.1），归集侧据此拒绝陈旧结果。
	Attempt int64
	// CorrelationID 配对请求与应答；异步实现用它做端到端诊断（§6.3）。
	CorrelationID string
	// Headers 承载 trace 传播等非业务元数据。
	Headers map[string]string
	Payload []byte
}

// Handler 处理订阅到的信封。返回错误表示本次处理失败：best-effort 契约下实现只需记录指标，
// 不要求重投——是否需要重跑由 PG 对账（R1–R5）决定，而不是由通道决定。
type Handler func(ctx context.Context, env Envelope) error

// Subscription 是订阅句柄；Close 之后实现不得再回调 Handler。Close 幂等。
type Subscription interface {
	Close() error
}

// Publisher 单向发送：既覆盖 Redis list/zset/stream/pubsub，也覆盖全双工 stream 的发送侧。
type Publisher interface {
	Send(ctx context.Context, env Envelope) error
}

// Subscriber 订阅一个 topic 并持续回调。
type Subscriber interface {
	Subscribe(ctx context.Context, topic Topic, h Handler) (Subscription, error)
}

// Requester 是半双工 request/reply：发送请求并阻塞等待一个应答（unary RPC）。
type Requester interface {
	Call(ctx context.Context, env Envelope) (Envelope, error)
}

// Channel 是所有实现的最小契约：能力用 Capabilities 声明，不要用类型断言猜（§9.3）。
type Channel interface {
	Kind() Kind
	Capabilities() Capabilities
}

// ErrUnsupported 表示实现不具备被请求的能力，调用方应回落到该任务配置的其他 channel，
// 而不是在本实现上重试。
var ErrUnsupported = errors.New("eventbus/channel: 实现不支持该能力")
