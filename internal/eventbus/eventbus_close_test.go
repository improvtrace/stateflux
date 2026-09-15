package eventbus

import (
	"context"
	"testing"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// closableChannel 是最小的可关闭 channel 替身。
type closableChannel struct{ closed int }

func (c *closableChannel) Call(context.Context, Envelope) (Envelope, error) { return Envelope{}, nil }
func (c *closableChannel) Kind() channel.Kind                               { return channel.KindMemory }
func (c *closableChannel) Capabilities() channel.Capabilities               { return channel.Capabilities{} }
func (c *closableChannel) Close() error                                     { c.closed++; return nil }

func TestEventBusCloseClosesEachChannelOnce(t *testing.T) {
	closer := &closableChannel{}
	bus, err := NewEventBus(map[string]channel.Channel{
		"a": closer,
		"b": closer, // 同一实例重复注册，只应关闭一次
	})
	if err != nil {
		t.Fatalf("new eventbus: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if closer.closed != 1 {
		t.Fatalf("closed = %d, want 1", closer.closed)
	}
	// 幂等：再次关闭不会重复调用。
	_ = bus.Close()
	if closer.closed != 1 {
		t.Fatalf("closed = %d after second Close, want 1", closer.closed)
	}
}
