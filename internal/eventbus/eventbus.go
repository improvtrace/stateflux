package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// EventKind 是节点事件类别（§3.2）。eventbus 订阅的目标是节点：一个节点有两类
// 事件支持订阅——system（系统事件）与 result（结果事件，ResultEvent 汇集）。
type EventKind string

const (
	// EventSystem 是节点系统事件：控制指令、生命周期与运维信号（§6.2）。
	EventSystem EventKind = "system"
	// EventResult 是节点结果事件：worker 发布的 ResultEvent 汇集（§5.5）。
	EventResult EventKind = "result"
)

// NodeTopic 返回某节点某类事件的 topic：node.{nodeID}.{event}（§3.2）。
func NodeTopic(nodeID string, kind EventKind) channel.Topic {
	return channel.Topic("node." + nodeID + "." + string(kind))
}

// TaskTopic 返回任务分发 topic：task.{band}（§5.3）。band 是数值 priority 的派生档位
// （schema.BandOf：low/normal/high）；topic 只做投递分组，精确顺序由 PG 的 priority
// 排序决定（§5.2）。
func TaskTopic(band string) channel.Topic { return channel.Topic("task." + band) }

// ResultTopic 返回结果归集 topic：result（§5.5）。同步 RPC 响应与异步 worker 结果都
// 适配为 ResultEvent 后发布到该 topic，由 Collector 统一订阅。
func ResultTopic() channel.Topic { return channel.Topic("result") }

// 通信面直接复用 channel 契约，不在其上再造一层类型。
type (
	// Envelope 是通道信封（§3.3）。
	Envelope = channel.Envelope
	// Handler 处理订阅到的信封。
	Handler = channel.Handler
	// Subscription 是订阅句柄。
	Subscription = channel.Subscription
)

// EventBus 是 eventbus 实例：创建时注册 channel，运行期可增删与解析（管理），
// 并面向节点提供事件订阅。可以订阅的前提是目标 channel 已在本实例注册。
// EventBus 不承诺持久化与投递语义——契约由 channel 声明，收敛由 PG 对账完成（§3.2）。
type EventBus struct {
	mu       sync.RWMutex
	channels map[string]channel.Channel
}

// NewEventBus 创建 eventbus 实例：channels 在创建时注册（逻辑 channel 名 → 实现）。
// 注册项非法（空名、nil 实现、重名）时返回错误。
func NewEventBus(channels map[string]channel.Channel) (*EventBus, error) {
	b := &EventBus{channels: make(map[string]channel.Channel, len(channels))}
	for name, ch := range channels {
		if err := b.Register(name, ch); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// Register 注册（或替换）一个 channel。空名与 nil 实现在装配期直接报错（尽早失败）。
func (b *EventBus) Register(name string, ch channel.Channel) error {
	if name == "" {
		return errors.New("eventbus: channel name must not be empty")
	}
	if ch == nil {
		return fmt.Errorf("eventbus: channel %q has a nil implementation", name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.channels[name] = ch
	return nil
}

// Unregister 注销一个 channel；未注册时返回错误。
func (b *EventBus) Unregister(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.channels[name]; !ok {
		return fmt.Errorf("eventbus: unregistered channel %q", name)
	}
	delete(b.channels, name)
	return nil
}

// Resolve 解析任务行记录的 channel 名；未注册返回错误，调度侧据此告警而非猜测。
func (b *EventBus) Resolve(name string) (channel.Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.channels[name]
	if !ok {
		return nil, fmt.Errorf("eventbus: unregistered channel %q", name)
	}
	return ch, nil
}

// Subscribe 订阅某节点的一类事件（system / result）：按 channel 名解析实现，
// 在 node.{nodeID}.{event} topic 上建立订阅。channel 未注册或实现不支持订阅时返回错误。
func (b *EventBus) Subscribe(ctx context.Context, channelName, nodeID string, kind EventKind, h Handler) (Subscription, error) {
	ch, err := b.Resolve(channelName)
	if err != nil {
		return nil, err
	}
	sub, ok := ch.(channel.Subscriber)
	if !ok {
		return nil, fmt.Errorf("eventbus: channel %s does not support subscribe: %w", channelName, channel.ErrUnsupported)
	}
	return sub.Subscribe(ctx, NodeTopic(nodeID, kind), h)
}

// Call 经指定 channel 发送一个信封并等待返回：应答语义由返回信封标记
// （见 channel.Requester）——应答型实现返回真实应答，单向实现返回零值信封。
func (b *EventBus) Call(ctx context.Context, channelName string, env Envelope) (Envelope, error) {
	ch, err := b.Resolve(channelName)
	if err != nil {
		return Envelope{}, err
	}
	return ch.Call(ctx, env)
}

// Send 经指定 channel 单向发送：等价于忽略应答信封的 Call。返回 nil 仅表示
// 「已尝试发送」，任何发送错误都不得被推断为任务未执行（§5.3）。
func (b *EventBus) Send(ctx context.Context, channelName string, env Envelope) error {
	_, err := b.Call(ctx, channelName, env)
	return err
}
