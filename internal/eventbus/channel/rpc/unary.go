package rpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// Unary 是半双工 request/reply 的 gRPC adapter（§7）：把 channel.Envelope 与
// ExecutorService.Execute 互相适配。
//
// 编解码约定（只做协议适配，不解释任务语义）：
//   - 入参 env.Payload 是 taskv1.TaskMessage 的 protobuf 二进制；
//   - 返回信封的 Payload 是 taskv1.ResultEvent 的 protobuf 二进制。
//
// env.Target 必须是执行节点的 gRPC 地址（host:port）。
type Unary struct {
	dialer  *Dialer
	timeout time.Duration
}

// NewUnary 构造 unary 通道；timeout <= 0 时使用调用方 ctx 的截止时间。
func NewUnary(dialer *Dialer, timeout time.Duration) *Unary {
	return &Unary{dialer: dialer, timeout: timeout}
}

// Kind 实现 channel.Channel。
func (*Unary) Kind() channel.Kind { return channel.KindRPCUnary }

// Capabilities 实现 channel.Channel：半双工 request/reply；RPC 返回成功不表示任务已完成。
func (*Unary) Capabilities() channel.Capabilities {
	return channel.Capabilities{RequestReply: true}
}

// Call 发送 TaskMessage 并阻塞等待一个 ResultEvent。
func (u *Unary) Call(ctx context.Context, env channel.Envelope) (channel.Envelope, error) {
	if env.Target == "" {
		return channel.Envelope{}, ErrNoTarget
	}
	msg := &taskv1.TaskMessage{}
	if len(env.Payload) > 0 {
		if err := proto.Unmarshal(env.Payload, msg); err != nil {
			return channel.Envelope{}, err
		}
	}
	if msg.GetTaskId() == 0 {
		msg.TaskId = parseKey(env.Key)
	}
	if msg.GetAttempt() == 0 {
		msg.Attempt = env.Attempt
	}
	conn, err := u.dialer.Conn(env.Target)
	if err != nil {
		return channel.Envelope{}, err
	}
	callCtx, cancel := withTimeout(ctx, u.timeout)
	defer cancel()

	resp, err := taskv1.NewExecutorServiceClient(conn).Execute(callCtx, &taskv1.ExecuteRequest{Task: msg})
	if err != nil {
		return channel.Envelope{}, err
	}
	return resultEnvelope(resp.GetResult())
}

// resultEnvelope 把 ResultEvent 适配为返回信封：保留 task_id/attempt 供归集侧 fencing。
func resultEnvelope(ev *taskv1.ResultEvent) (channel.Envelope, error) {
	if ev == nil {
		return channel.Envelope{Topic: channel.Topic("result")}, nil
	}
	payload, err := proto.Marshal(ev)
	if err != nil {
		return channel.Envelope{}, err
	}
	return channel.Envelope{
		Topic:   channel.Topic("result"),
		Key:     itoa(ev.GetTaskId()),
		Attempt: ev.GetAttempt(),
		Target:  ev.GetSource(),
		Payload: payload,
	}, nil
}
