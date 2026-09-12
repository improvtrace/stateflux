package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// defaultMemoryBuffer 是每个订阅的缓冲容量：缓冲满时 Send 按 ctx 阻塞（背压），
// 不静默丢弃——「丢消息」的演练由测试使用方注入，而不是由基座偷偷模拟。
const defaultMemoryBuffer = 64

// Memory 是 §12.1 的 in-memory fake：scheduler / collector / worker 的测试通信基座，
// 生产装配不得使用。
//
// 它同时声明三种能力，因此同一实现既能驱动半双工路径（Handle + Call），也能驱动单向与
// 全双工路径（Send + Subscribe）；文档中「同步与异步只是 channel 差异」的说法因此可以在
// 单进程内对照演练。它不模拟中间件故障：丢消息、重复投递、发送结果不确定由使用方在
// Handler 内或自定义实现中制造，避免基座替测试决定语义。
type Memory struct {
	mu      sync.RWMutex
	subs    map[Topic]map[*memorySub]struct{}
	reqs    map[Topic]func(context.Context, Envelope) (Envelope, error)
	bufSize int
}

// NewMemory 创建 in-memory channel。
func NewMemory() *Memory {
	return &Memory{
		subs:    make(map[Topic]map[*memorySub]struct{}),
		reqs:    make(map[Topic]func(context.Context, Envelope) (Envelope, error)),
		bufSize: defaultMemoryBuffer,
	}
}

// Kind 实现 Channel。
func (*Memory) Kind() Kind { return KindMemory }

// Capabilities 实现 Channel：fake 声明订阅、request/reply 与全双工；LocalAck 保持 false
// ——投递确认不参与正确性判断，fake 也不假装提供可靠性。
func (*Memory) Capabilities() Capabilities {
	return Capabilities{Subscribe: true, RequestReply: true, FullDuplex: true}
}

// Handle 为 topic 注册 request/reply 处理函数（半双工：一个请求对应一个应答），
// 对应生产形态里的 unary Execute RPC（§3.2）。
func (m *Memory) Handle(topic Topic, fn func(context.Context, Envelope) (Envelope, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs[topic] = fn
}

// Subscribe 订阅 topic（Subscriber 能力）：投递在独立 goroutine 中进行，Close 或 ctx
// 结束时停止回调。
func (m *Memory) Subscribe(ctx context.Context, topic Topic, h Handler) (Subscription, error) {
	if h == nil {
		return nil, errors.New("eventbus/channel: Subscribe 需要非空 Handler")
	}
	sub := &memorySub{
		mem:   m,
		topic: topic,
		ch:    make(chan Envelope, m.bufSize),
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

// Send 向 topic 的所有订阅者投递（Publisher 能力）。没有订阅者时返回 nil——best-effort
// 契约下「没有人在听」不是错误；缓冲满时按 ctx 阻塞，ctx 结束时返回其错误，调用方不得据此
// 推断任务是否被执行（§5.3）。
func (m *Memory) Send(ctx context.Context, env Envelope) error {
	m.mu.RLock()
	subs := make([]*memorySub, 0, len(m.subs[env.Topic]))
	for sub := range m.subs[env.Topic] {
		subs = append(subs, sub)
	}
	m.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.ch <- env:
		case <-sub.done:
			// 订阅已关闭：跳过，best-effort 契约允许。
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Call 发送请求并阻塞等待一个应答（Requester 能力）：先发送、再读取一个结果。
func (m *Memory) Call(ctx context.Context, env Envelope) (Envelope, error) {
	m.mu.RLock()
	fn := m.reqs[env.Topic]
	m.mu.RUnlock()
	if fn == nil {
		return Envelope{}, fmt.Errorf("eventbus/channel: topic %q 未注册 request handler: %w", env.Topic, ErrUnsupported)
	}

	type reply struct {
		env Envelope
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
		return Envelope{}, ctx.Err()
	}
}

type memorySub struct {
	mem   *Memory
	topic Topic
	ch    chan Envelope
	done  chan struct{}
	once  sync.Once
}

// Close 实现 Subscription，且幂等。
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
