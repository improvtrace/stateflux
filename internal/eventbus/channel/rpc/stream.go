package rpc

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// Stream 是双向 ResultStream 的**发送侧** adapter（§5.4、§7）：worker 经它与调度节点
// 建立长连接，发布 ResultEvent 并等待 ResultAck。服务端（Collector 订阅）由 biz 承载，
// 不在本 adapter 内——本包只做协议编解码（§15.1#8）。
//
// 断线重连：发送失败或流错误时丢弃当前流，下一次 Call 重建并由调用方（worker WAL）重发
// 未确认结果。Call 返回错误**不代表任务未执行**，收敛仍由 R1 完成（§5.3）。
//
// 编解码约定：env.Payload 是 taskv1.ResultEvent 的 protobuf 二进制；返回信封的 Payload 是
// taskv1.ResultAck 的 protobuf 二进制（Call 成功即表示收到 ack）。
type Stream struct {
	dialer  *Dialer
	timeout time.Duration

	mu      sync.Mutex
	streams map[string]*streamConn
	closed  bool
}

// NewStream 构造 stream 通道。
func NewStream(dialer *Dialer, timeout time.Duration) *Stream {
	return &Stream{dialer: dialer, timeout: timeout, streams: map[string]*streamConn{}}
}

// Kind 实现 channel.Channel。
func (*Stream) Kind() channel.Kind { return channel.KindRPCStream }

// Capabilities：发送侧声明 request/ack（结果确认是流内信号，不构成可靠性条件）。
func (*Stream) Capabilities() channel.Capabilities {
	return channel.Capabilities{RequestReply: true}
}

// Call 发送一个 ResultEvent 并等待 ack。
func (s *Stream) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	if env.Target == "" {
		return channel.Envelope{}, ErrNoTarget
	}
	ev := &taskv1.ResultEvent{}
	if len(env.Payload) > 0 {
		if err := proto.Unmarshal(env.Payload, ev); err != nil {
			return channel.Envelope{}, err
		}
	}
	if ev.GetTaskId() == 0 {
		ev.TaskId = parseKey(env.Key)
	}
	if ev.GetAttempt() == 0 {
		ev.Attempt = env.Attempt
	}

	sc, err := s.conn(env.Target)
	if err != nil {
		return channel.Envelope{}, err
	}
	callCtx, cancel := withTimeout(ctx, s.timeout)
	defer cancel()
	ack, err := sc.send(callCtx, ev)
	if err != nil {
		s.drop(env.Target, sc)
		return channel.Envelope{}, err
	}
	payload, err := proto.Marshal(ack)
	if err != nil {
		return channel.Envelope{}, err
	}
	return channel.Envelope{
		Topic:   channel.Topic("result"),
		Key:     itoa(ev.GetTaskId()),
		Attempt: ev.GetAttempt(),
		Target:  env.Target,
		Payload: payload,
	}, nil
}

// conn 返回（必要时建立）到目标地址的流。
func (s *Stream) conn(address string) (*streamConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("eventbus/channel/rpc: stream closed")
	}
	if sc, ok := s.streams[address]; ok && !sc.isClosed() {
		return sc, nil
	}
	conn, err := s.dialer.Conn(address)
	if err != nil {
		return nil, err
	}
	stream, err := taskv1.NewExecutorServiceClient(conn).ResultStream(context.Background())
	if err != nil {
		return nil, err
	}
	sc := newStreamConn(address, stream)
	s.streams[address] = sc
	return sc, nil
}

// drop 丢弃失败的流（下次 Call 重建）。
func (s *Stream) drop(address string, sc *streamConn) {
	sc.close()
	s.mu.Lock()
	if s.streams[address] == sc {
		delete(s.streams, address)
	}
	s.mu.Unlock()
}

// Close 关闭全部流。
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, sc := range s.streams {
		sc.close()
	}
	s.streams = map[string]*streamConn{}
	return nil
}

// streamConn 是单条 ResultStream 的发送/确认状态。
type streamConn struct {
	address string
	stream  taskv1.ExecutorService_ResultStreamClient

	sendMu sync.Mutex // 串行化 Send，保证 gRPC 流的线程安全

	mu      sync.Mutex
	waiters map[string]chan *taskv1.ResultAck
	closed  bool
	recvErr error
}

func newStreamConn(address string, stream taskv1.ExecutorService_ResultStreamClient) *streamConn {
	sc := &streamConn{address: address, stream: stream, waiters: map[string]chan *taskv1.ResultAck{}}
	go sc.recvLoop()
	return sc
}

func ackKey(id, attempt int64) string {
	return strconv.FormatInt(id, 10) + ":" + strconv.FormatInt(attempt, 10)
}

// send 发送事件并等待对应 ack。
func (sc *streamConn) send(ctx context.Context, ev *taskv1.ResultEvent) (*taskv1.ResultAck, error) {
	key := ackKey(ev.GetTaskId(), ev.GetAttempt())
	ch := make(chan *taskv1.ResultAck, 1)

	sc.mu.Lock()
	if sc.closed {
		err := sc.recvErr
		sc.mu.Unlock()
		if err == nil {
			err = errors.New("eventbus/channel/rpc: stream closed")
		}
		return nil, err
	}
	sc.waiters[key] = ch
	sc.mu.Unlock()

	sc.sendMu.Lock()
	err := sc.stream.Send(ev)
	sc.sendMu.Unlock()
	if err != nil {
		sc.remove(key)
		return nil, err
	}

	select {
	case ack := <-ch:
		return ack, nil
	case <-ctx.Done():
		sc.remove(key)
		return nil, ctx.Err()
	}
}

func (sc *streamConn) remove(key string) {
	sc.mu.Lock()
	delete(sc.waiters, key)
	sc.mu.Unlock()
}

func (sc *streamConn) recvLoop() {
	for {
		ack, err := sc.stream.Recv()
		if err != nil {
			sc.mu.Lock()
			sc.closed = true
			sc.recvErr = err
			for key, ch := range sc.waiters {
				close(ch)
				delete(sc.waiters, key)
			}
			sc.mu.Unlock()
			return
		}
		key := ackKey(ack.GetTaskId(), ack.GetAttempt())
		sc.mu.Lock()
		ch, ok := sc.waiters[key]
		if ok {
			delete(sc.waiters, key)
		}
		sc.mu.Unlock()
		if ok {
			ch <- ack
		}
	}
}

func (sc *streamConn) isClosed() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.closed
}

func (sc *streamConn) close() {
	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		return
	}
	sc.closed = true
	for key, ch := range sc.waiters {
		close(ch)
		delete(sc.waiters, key)
	}
	sc.mu.Unlock()
	_ = sc.stream.CloseSend()
}

// 编译期断言：两个 adapter 都满足 channel.Channel。
var (
	_ channel.Channel = (*Unary)(nil)
	_ channel.Channel = (*Stream)(nil)
)
