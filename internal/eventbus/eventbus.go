package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/improvtrace/stateflux/internal/cluster"
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

// NodeView 是集群节点列表的动态视图（接口注入）：直接复用 cluster.ClusterCacheView
// ——NewCache 的返回类型即该接口，装配时把它注入即可。EventBus 不持有节点快照，
// 每次解析都读注入实例——节点上线/下线由该实例的实现方负责刷新。
type NodeView = cluster.ClusterCacheView

// ChannelSpec 是「待创建 channel」的规格：EventBus 据此经 ChannelFactory 惰性创建实现。
// Name 是缓存键（逻辑 channel 名），Kind/DSN 是拨号要素。
type ChannelSpec struct {
	// Name 是逻辑 channel 名：task 行记录的、队列映射表给出的同一命名空间。
	Name string
	// Kind 是期望的实现形态（channel.Kind）。
	Kind channel.Kind
	// DSN 是拨号要素：Redis 形态承载队列键/命名空间，RPC 形态承载目标节点地址。
	// 经节点寻址（QueueForNode）解析且 DSN 为空时，由 EventBus 用节点地址补齐。
	DSN string
}

// QueueRegistry 是集群异步消息队列节点映射表（动态，接口注入）：逻辑 channel 名 ↔
// 节点队列的映射。映射随节点拓扑变化，实现方负责刷新；EventBus 只在需要时查询。
// 方法带 ctx，因为实现通常要读远端共识信息（redis 中的队列路由）。
type QueueRegistry interface {
	// Spec 返回逻辑 channel 名（队列名）的创建规格；false 表示该名字不是已知队列，
	// 调用侧报错而非猜测实现。
	Spec(ctx context.Context, name string) (ChannelSpec, bool)
	// QueueForNode 返回某节点承载的异步队列 channel 名（节点寻址）；false 表示该节点
	// 当前没有队列映射。
	QueueForNode(ctx context.Context, nodeID string) (string, bool)
	// Default 返回默认队列 channel 规格：target 为空的队列语义投递使用。false 表示
	// 未配置默认队列，此时 target 为空的调用必须显式指定 channel。
	Default(ctx context.Context) (ChannelSpec, bool)
}

// ChannelFactory 按 spec 创建（拨号）channel 实现。由装配侧提供（按 Kind 分派到
// redis/rpc/mem 的构造函数），EventBus 不了解任何具体实现细节。
type ChannelFactory interface {
	Open(ctx context.Context, spec ChannelSpec) (channel.Channel, error)
}

// ChannelFactoryFunc 是 ChannelFactory 的函数适配器。
type ChannelFactoryFunc func(ctx context.Context, spec ChannelSpec) (channel.Channel, error)

// Open 实现 ChannelFactory。
func (f ChannelFactoryFunc) Open(ctx context.Context, spec ChannelSpec) (channel.Channel, error) {
	return f(ctx, spec)
}

// ErrClosed 表示 EventBus 已关闭，拒绝新的订阅与发送。
var ErrClosed = errors.New("eventbus: closed")

// Options 是 Subscribe / SubscribeTopic / Send / Call 的调用期选项。
type Options struct {
	// Channel 显式指定逻辑 channel 名：优先级最高，命中后不再经 QueueRegistry 解析，
	// 但仍走惰性创建与缓存。空表示自动解析。
	Channel string
}

// Option 是调用期选项的 setter。
type Option func(*Options)

// WithChannel 指定本次调用使用的逻辑 channel 名。
func WithChannel(name string) Option {
	return func(o *Options) { o.Channel = name }
}

// EventBus 是 eventbus 实例：依赖两个动态注入的集群数据源——节点列表（NodeView）与
// 异步消息队列节点映射表（QueueRegistry）——在订阅/发送/调用时按需创建对应 Channel
// （不存在才创建，存在则复用缓存）。预注册（Register）的静态 channel 优先于惰性创建。
// EventBus 不承诺持久化与投递语义——契约由 channel 声明，收敛由 PG 对账完成（§3.2）。
type EventBus struct {
	view    NodeView
	queues  QueueRegistry
	factory ChannelFactory

	// defaultName 是未显式指定 channel 且无节点寻址时优先使用的逻辑 channel 名；
	// 空则回落到 QueueRegistry.Default()。
	defaultName string

	mu       sync.RWMutex
	channels map[string]channel.Channel
	closed   bool
}

// NewEventBus 创建 eventbus 实例：注入集群节点视图、队列映射表与 channel 工厂。
// 三者均不可为 nil（装配期尽早失败）；channels 是预注册的静态通道，defaultName 指定
// 静态默认通道（必须已预注册，空表示由 QueueRegistry.Default 决定）。
func NewEventBus(view NodeView, queues QueueRegistry, factory ChannelFactory, channels map[string]channel.Channel, defaultName string) (*EventBus, error) {
	if view == nil {
		return nil, errors.New("eventbus: nil node view")
	}
	if queues == nil {
		return nil, errors.New("eventbus: nil queue registry")
	}
	if factory == nil {
		return nil, errors.New("eventbus: nil channel factory")
	}
	return newEventBus(view, queues, factory, channels, defaultName)
}

// NewStatic 创建「静态装配」的 eventbus：只使用预注册 channel，不做惰性创建，也不依赖
// 集群节点列表与队列映射表（单机部署、管理面与测试）。未预注册的名字一律报错。
func NewStatic(channels map[string]channel.Channel, defaultName string) (*EventBus, error) {
	return newEventBus(nil, nil, nil, channels, defaultName)
}

func newEventBus(view NodeView, queues QueueRegistry, factory ChannelFactory, channels map[string]channel.Channel, defaultName string) (*EventBus, error) {
	b := &EventBus{
		view:        view,
		queues:      queues,
		factory:     factory,
		defaultName: defaultName,
		channels:    make(map[string]channel.Channel, len(channels)+8),
	}
	for name, ch := range channels {
		if err := b.Register(name, ch); err != nil {
			return nil, err
		}
	}
	if defaultName != "" {
		if _, ok := b.channels[defaultName]; !ok {
			return nil, fmt.Errorf("eventbus: default channel %q is not pre-registered", defaultName)
		}
	}
	return b, nil
}

// Register 注册（或替换）一个静态 channel。空名与 nil 实现在装配期直接报错。
// 静态 channel 不参与惰性创建，但被调用路径缓存复用。
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

// Resolve 解析已存在的 channel（静态注册或已惰性创建）；不存在时不创建，返回错误。
// 需要「解析或创建」的调用请用 Channel。
func (b *EventBus) Resolve(name string) (channel.Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.channels[name]
	if !ok {
		return nil, fmt.Errorf("eventbus: unresolved channel %q", name)
	}
	return ch, nil
}

// Channel 返回名字对应的 channel：已存在（预注册或早先创建）则复用，否则经
// QueueRegistry 解析规格并由 ChannelFactory 惰性创建。供需要直接操作 channel
// （如读 Capabilities）的调用侧使用。
func (b *EventBus) Channel(ctx context.Context, name string) (channel.Channel, error) {
	return b.acquire(ctx, resolved{name: name})
}

// Invalidate 丢弃一个已创建/注册的 channel 缓存（节点下线、DSN 变更时由管理侧调用）；
// 不关闭底层实现——是否可复用由调用方决定。未存在时返回错误。
func (b *EventBus) Invalidate(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.channels[name]; !ok {
		return fmt.Errorf("eventbus: unknown channel %q", name)
	}
	delete(b.channels, name)
	return nil
}

// resolved 是一次调用解析出的目标：逻辑 channel 名 + 可选的节点寻址身份。
type resolved struct {
	name   string
	nodeID string
}

// acquire 返回目标对应的 channel：缓存命中直接复用；未命中时解析 spec 并经
// ChannelFactory 惰性创建（双检锁，保证同名只创建一次）。创建失败不缓存，下次调用重试。
func (b *EventBus) acquire(ctx context.Context, r resolved) (channel.Channel, error) {
	b.mu.RLock()
	ch, ok := b.channels[r.name]
	closed := b.closed
	b.mu.RUnlock()
	if ok {
		return ch, nil
	}
	if closed {
		return nil, ErrClosed
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.channels[r.name]; ok {
		return ch, nil
	}
	if b.closed {
		return nil, ErrClosed
	}
	spec, err := b.specFor(ctx, r)
	if err != nil {
		return nil, err
	}
	if b.factory == nil {
		return nil, fmt.Errorf("eventbus: channel %q is not pre-registered and no channel factory is configured", r.name)
	}
	ch, err = b.factory.Open(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("eventbus: open channel %q (kind=%s): %w", spec.Name, spec.Kind, err)
	}
	b.channels[r.name] = ch
	return ch, nil
}

// specFor 把解析结果转成创建规格：先查队列映射表；命中节点寻址时校验节点在集群视图
// 中存在，并用节点地址补齐空 DSN（RPC 形态直连目标节点）。
func (b *EventBus) specFor(ctx context.Context, r resolved) (ChannelSpec, error) {
	if b.queues == nil {
		return ChannelSpec{}, fmt.Errorf("eventbus: channel %q is not pre-registered and no queue registry is configured", r.name)
	}
	spec, ok := b.queues.Spec(ctx, r.name)
	if !ok {
		if def, ok2 := b.queues.Default(ctx); ok2 && def.Name == r.name {
			spec, ok = def, true
		}
	}
	if !ok {
		return ChannelSpec{}, fmt.Errorf("eventbus: no queue mapping for channel %q", r.name)
	}
	if r.nodeID == "" || b.view == nil {
		return spec, nil
	}
	node, ok := b.view.Node(r.nodeID)
	if !ok {
		return ChannelSpec{}, fmt.Errorf("eventbus: unknown cluster node %q", r.nodeID)
	}
	if spec.DSN == "" {
		spec.DSN = node.Address
	}
	return spec, nil
}

// resolve 把一次调用解析为逻辑 channel 名：
//  1. options 显式指定（WithChannel）——最高优先级；
//  2. target 非空——经 QueueRegistry 查该节点的异步队列映射（节点寻址）；
//  3. 其余——静态默认通道，其次 QueueRegistry.Default()。
func (b *EventBus) resolve(ctx context.Context, opts Options, target string) (resolved, error) {
	if opts.Channel != "" {
		return resolved{name: opts.Channel}, nil
	}
	if target != "" {
		if b.queues == nil {
			return resolved{}, fmt.Errorf("eventbus: node %q addressing requires a queue registry", target)
		}
		name, ok := b.queues.QueueForNode(ctx, target)
		if !ok {
			return resolved{}, fmt.Errorf("eventbus: node %q has no queue mapping", target)
		}
		return resolved{name: name, nodeID: target}, nil
	}
	if b.defaultName != "" {
		return resolved{name: b.defaultName}, nil
	}
	if b.queues != nil {
		if spec, ok := b.queues.Default(ctx); ok {
			return resolved{name: spec.Name}, nil
		}
	}
	return resolved{}, errors.New("eventbus: no default channel; specify one via WithChannel")
}

func applyOptions(opts []Option) Options {
	var o Options
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// SubscribeTopic 在任意 topic 上订阅：按 options（或默认 channel）解析并惰性创建 channel，
// 要求该实现具备 Subscriber 能力。任务队列订阅（task.{band}）走这里。
func (b *EventBus) SubscribeTopic(ctx context.Context, topic channel.Topic, h Handler, opts ...Option) (Subscription, error) {
	return b.subscribe(ctx, topic, h, "", opts)
}

// Subscribe 订阅某节点的一类事件（system / result）：按节点经 QueueRegistry 解析队列
// channel（或经 options 指定），惰性创建后在 node.{nodeID}.{event} topic 上建立订阅。
func (b *EventBus) Subscribe(ctx context.Context, nodeID string, kind EventKind, h Handler, opts ...Option) (Subscription, error) {
	return b.subscribe(ctx, NodeTopic(nodeID, kind), h, nodeID, opts)
}

func (b *EventBus) subscribe(ctx context.Context, topic channel.Topic, h Handler, target string, opts []Option) (Subscription, error) {
	r, err := b.resolve(ctx, applyOptions(opts), target)
	if err != nil {
		return nil, err
	}
	ch, err := b.acquire(ctx, r)
	if err != nil {
		return nil, err
	}
	sub, ok := ch.(channel.Subscriber)
	if !ok {
		return nil, fmt.Errorf("eventbus: channel %s does not support subscribe: %w", r.name, channel.ErrUnsupported)
	}
	return sub.Subscribe(ctx, topic, h)
}

// Call 经解析出的 channel 发送一个信封并等待返回：target 非空时按节点队列映射寻址，
// 应答语义由返回信封标记（见 channel.Requester）。
func (b *EventBus) Call(ctx context.Context, env Envelope, opts ...Option) (Envelope, error) {
	r, err := b.resolve(ctx, applyOptions(opts), env.Target)
	if err != nil {
		return Envelope{}, err
	}
	ch, err := b.acquire(ctx, r)
	if err != nil {
		return Envelope{}, err
	}
	return ch.Call(ctx, env)
}

// Send 经解析出的 channel 单向发送：等价于忽略应答信封的 Call。返回 nil 仅表示
// 「已尝试发送」，任何发送错误都不得被推断为任务未执行（§5.3）。
func (b *EventBus) Send(ctx context.Context, env Envelope, opts ...Option) error {
	_, err := b.Call(ctx, env, opts...)
	return err
}

// Close 关闭全部缓存中的 channel（幂等），供进程优雅退出调用。只有实现 Close 的
// channel 会被关闭；同一实例重复注册只关一次。关闭后不再创建新通道。
func (b *EventBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	channels := make([]channel.Channel, 0, len(b.channels))
	seen := make(map[channel.Channel]struct{}, len(b.channels))
	for _, ch := range b.channels {
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		channels = append(channels, ch)
	}
	b.channels = make(map[string]channel.Channel)
	b.mu.Unlock()

	var errs []error
	for _, ch := range channels {
		closer, ok := ch.(interface{ Close() error })
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("eventbus: close %T: %w", ch, err))
		}
	}
	return errors.Join(errs...)
}
