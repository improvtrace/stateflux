package mem

import (
	"context"
	"errors"
	"sync"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// defaultMemoryBuffer 是每个订阅的缓冲容量：缓冲满时 Send 按 ctx 阻塞（背压），
// 不静默丢弃——「丢消息」的演练由测试使用方注入，而不是由基座偷偷模拟。
const defaultMemoryBuffer = 64

// Memory 是 §12.1 的 in-memory fake（本包 mem，channel 契约的实现子目录）：scheduler / collector / worker 的测试通信基座，
// 生产装配不得使用。
//
// 它同时声明三种能力，因此同一实现既能驱动半双工路径（Handle + Call），也能驱动单向与
// 全双工路径（Call + Subscribe；单向路径的应答为零值信封）；文档中「同步与异步只是
// channel 差异」的说法因此可以在单进程内对照演练。它不模拟中间件故障：丢消息、重复投递、
// 发送结果不确定由使用方在 channel.Handler 内或自定义实现中制造，避免基座替测试决定语义。
type Memory struct {
	mu      sync.RWMutex
	subs    map[channel.Topic]map[*memorySub]struct{}
	reqs    map[channel.Topic]func(context.Context, channel.Envelope) (channel.Envelope, error)
	bufSize int
}

// NewMemory 创建 in-memory channel。
func NewMemory() *Memory {
	return &Memory{
		subs:    make(map[channel.Topic]map[*memorySub]struct{}),
		reqs:    make(map[channel.Topic]func(context.Context, channel.Envelope) (channel.Envelope, error)),
		bufSize: defaultMemoryBuffer,
	}
}

// channel.Kind 实现 channel.Channel。
func (*Memory) Kind() channel.Kind { return channel.KindMemory }

// channel.Capabilities 实现 channel.Channel：fake 声明订阅、request/reply 与全双工；LocalAck 保持 false
// ——投递确认不参与正确性判断，fake 也不假装提供可靠性。
func (*Memory) Capabilities() channel.Capabilities {
	return channel.Capabilities{Subscribe: true, RequestReply: true, FullDuplex: true}
}

// Handle 为 topic 注册 request/reply 处理函数（半双工：一个请求对应一个应答），
// 对应生产形态里的 unary Execute RPC（§3.2）。
func (m *Memory) Handle(topic channel.Topic, fn func(context.Context, channel.Envelope) (channel.Envelope, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs[topic] = fn
}

// Subscribe 订阅 topic（Subscriber 能力）：投递在独立 goroutine 中进行，Close 或 ctx
// 结束时停止回调。
func (m *Memory) Subscribe(ctx context.Context, topic channel.Topic, h channel.Handler) (channel.Subscription, error) {
	if h == nil {
		return nil, errors.New("eventbus/channel: Subscribe requires a non-nil channel.Handler")
	}
	sub := &memorySub{
		mem:   m,
		topic: topic,
		ch:    make(chan channel.Envelope, m.bufSize),
		done:  make(chan struct{}),
	}
	m.mu.Lock()
	if m.subs[topic] == nil {
		m.subs[topic] = make(map[*memorySub]struct{})
	}
	m.subs[topic][sub] = struct{}{}
	m.mu.Unlock()

	go func() {
		defer sub.remove()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.done:
				return
			case env := <-sub.ch:
				// best-effort：处理失败只影响调用方自己的指标与对账，通道不重投。
				_ = h(ctx, env)
			}
		}
	}()
	return sub, nil
}

// Call 发送信封并阻塞等待返回（Requester 能力）：topic 注册了 request handler 时返回
// 真实应答（半双工路径）；未注册时投递给订阅者并返回零值信封——单向路径的「已尝试发送」
// 确认，标记语义见 channel.Requester。
func (m *Memory) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	m.mu.RLock()
	fn := m.reqs[env.Topic]
	m.mu.RUnlock()
	if fn == nil {
		// 单向路径：投递给订阅者（没有订阅者也是 best-effort 的合法状态），应答为零值。
		subs := make([]*memorySub, 0, len(m.subs[env.Topic]))
		for sub := range m.subs[env.Topic] {
			subs = append(subs, sub)
		}
		for _, sub := range subs {
			select {
			case sub.ch <- env:
			case <-sub.done:
				// 订阅已关闭：跳过，best-effort 契约允许。
			case <-ctx.Done():
				return channel.Envelope{}, ctx.Err()
			}
		}
		return channel.Envelope{}, nil
	}

	type reply struct {
		env channel.Envelope
		err error
	}
	done := make(chan reply, 1)
	go func() {
		env, err := fn(ctx, env)
		done <- reply{env: env, err: err}
	}()

	select {
	case r := <-done:
		return r.env, r.err
	case <-ctx.Done():
		return channel.Envelope{}, ctx.Err()
	}
}

type memorySub struct {
	mem   *Memory
	topic channel.Topic
	ch    chan channel.Envelope
	done  chan struct{}
	once  sync.Once
}

// Close 实现 channel.Subscription，且幂等。
func (s *memorySub) Close() error {
	s.once.Do(func() { close(s.done) })
	s.remove()
	return nil
}

func (s *memorySub) remove() {
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()
	subs, ok := s.mem.subs[s.topic]
	if !ok {
		return
	}
	delete(subs, s)
	if len(subs) == 0 {
		delete(s.mem.subs, s.topic)
	}
}
