package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// ResultTopic 是结果 topic（§3.2）：worker 发布的结果与同步 RPC 适配出的 ResultEvent
// 汇入同一入口，Collector 订阅它（§5.5）。
const ResultTopic channel.Topic = "result"

// TaskTopic 返回某优先级档位的任务 topic：task.{band}（§3.2）。band 是数值 priority 的派生档位
// （schema.BandOf：low/normal/high），不是原始 0–100 数值——topic 只做订阅与投递分组，不该随业务
// 细分膨胀出上百个 topic；精确优先顺序仍由 PG 的 priority 排序决定（§5.2）。
func TaskTopic(band string) channel.Topic { return channel.Topic("task." + band) }

// 通信面直接复用 channel 契约，不在其上再造一层类型。
type (
	// Envelope 是通道信封（§3.3）。
	Envelope = channel.Envelope
	// Handler 处理订阅到的信封。
	Handler = channel.Handler
	// Subscription 是订阅句柄。
	Subscription = channel.Subscription
)

// Registry 把任务行记录的 channel 名解析为具体实现（§5.3）：装配期注册、运行期只读选择。
// 路由决策属于调度侧，Registry 只做名字到实现的翻译。
type Registry struct {
	mu     sync.RWMutex
	byName map[string]channel.Channel
}

// NewRegistry 创建空的 channel 注册表。
func NewRegistry() *Registry { return &Registry{byName: make(map[string]channel.Channel)} }

// Register 注册一个实现。空名、nil 实现与重名都在装配期直接报错（尽早失败）。
func (r *Registry) Register(name string, ch channel.Channel) error {
	if name == "" {
		return errors.New("eventbus: channel 名不能为空")
	}
	if ch == nil {
		return fmt.Errorf("eventbus: channel %q 的实现为 nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		return fmt.Errorf("eventbus: channel %q 重复注册", name)
	}
	r.byName[name] = ch
	return nil
}

// Resolve 解析任务行记录的 channel 名；未注册返回错误，调度侧据此告警而非猜测。
func (r *Registry) Resolve(name string) (channel.Channel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("eventbus: 未注册的 channel %q", name)
	}
	return ch, nil
}

// Bus 是选定 channel 之上的 EventBus 面：Send 发布、Subscribe 订阅（§3.2）。同一个契约
// 同时承载任务与结果：worker 订阅任务 topic，Collector 订阅 ResultTopic。
type Bus struct{ ch channel.Channel }

// New 在给定 channel 上构造 EventBus 面。
func New(ch channel.Channel) *Bus { return &Bus{ch: ch} }

// Channel 返回底层实现，供能力判定与指标标签使用。
func (b *Bus) Channel() channel.Channel { return b.ch }

// Send 发布一个信封，要求实现具备 Publisher 能力。返回 nil 仅表示「已尝试发送」，
// 不代表对方已收到、更不代表任务已执行（§3.2）；任何发送错误都不得被推断为任务未执行（§5.3）。
func (b *Bus) Send(ctx context.Context, env channel.Envelope) error {
	pub, ok := b.ch.(channel.Publisher)
	if !ok {
		return fmt.Errorf("eventbus: channel %s 不支持发送: %w", b.ch.Kind(), channel.ErrUnsupported)
	}
	return pub.Send(ctx, env)
}

// Subscribe 订阅一个 topic，要求实现具备 Subscriber 能力。
func (b *Bus) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	sub, ok := b.ch.(channel.Subscriber)
	if !ok {
		return nil, fmt.Errorf("eventbus: channel %s 不支持订阅: %w", b.ch.Kind(), channel.ErrUnsupported)
	}
	return sub.Subscribe(ctx, topic, h)
}
