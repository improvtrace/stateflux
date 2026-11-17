package rpc

import (
	"errors"
	"testing"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// recvAck 的契约：waiter channel 被关闭（流断开/主动关闭）时收到 nil ack，
// 必须返回错误而非 (nil, nil)——否则 Call 会把断线当成功，worker 误 Ack WAL 丢结果。
func TestRecvAckClosedChannelReturnsError(t *testing.T) {
	sc := &streamConn{waiters: map[string]chan *taskv1.ResultAck{}}

	// 场景 1：recvLoop 出错关闭 waiter（recvErr 已记录）。
	sc.recvErr = errors.New("connection reset")
	ack, err := recvAck(sc, nil)
	if ack != nil || err == nil {
		t.Fatalf("closed waiter with recvErr: got (%v, %v), want (nil, error)", ack, err)
	}
	if !errors.Is(err, sc.recvErr) {
		t.Fatalf("want recvErr propagated, got %v", err)
	}

	// 场景 2：主动 close() 关闭 waiter（无 recvErr，回落哨兵错误）。
	sc.recvErr = nil
	ack, err = recvAck(sc, nil)
	if ack != nil || !errors.Is(err, ErrClosedBeforeAck) {
		t.Fatalf("closed waiter without recvErr: got (%v, %v), want (nil, ErrClosedBeforeAck)", ack, err)
	}

	// 场景 3：正常 ack 原样返回。
	want := &taskv1.ResultAck{Accepted: true, TaskId: 42, Attempt: 1}
	got, err := recvAck(sc, want)
	if err != nil || got != want {
		t.Fatalf("valid ack: got (%v, %v), want (%v, nil)", got, err, want)
	}
}
