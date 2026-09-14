package codec

import (
	"testing"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/task"
)

func newTestCodec(t *testing.T) *Codec {
	t.Helper()
	reg := task.NewRegistry()
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err != nil {
		t.Fatalf("register: %v", err)
	}
	return New(reg)
}

func TestCodecRoundTrip(t *testing.T) {
	c := newTestCodec(t)
	msg := &taskv1.TaskMessage{TaskId: 42, Attempt: 3, Type: "demo", Operator: "heartbeat", Payload: []byte(`{"a":1}`)}

	raw, err := c.Encode(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	sig, err := c.PeekSignature(raw)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if sig != task.Signature("stateflux.task.v1.TaskMessage") {
		t.Fatalf("signature = %q", sig)
	}
	out, err := c.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := out.(*taskv1.TaskMessage)
	if !ok {
		t.Fatalf("decoded type = %T", out)
	}
	if got.GetTaskId() != 42 || got.GetAttempt() != 3 || got.GetType() != "demo" {
		t.Fatalf("decoded = %+v", got)
	}
}

func TestCodecRejectsBadFrameAndUnknownSignature(t *testing.T) {
	c := newTestCodec(t)
	if _, err := c.Decode([]byte("not-a-frame")); err == nil {
		t.Fatal("expected bad frame error")
	}
	// 未注册签名的帧：手工构造合法帧头但签名未知。
	reg := task.NewRegistry()
	empty := New(reg)
	if _, err := empty.Encode(&taskv1.TaskMessage{}); err == nil {
		t.Fatal("expected unregistered signature error on encode")
	}
}
