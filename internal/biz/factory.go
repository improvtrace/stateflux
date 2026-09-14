package biz

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/factory"
	"github.com/improvtrace/stateflux/internal/worker"
)

// 演示工厂与 handler 的任务标识（type 粗、operator 细，§3.1）。
const (
	DemoType         = "demo"
	DemoOperatorBeat = "heartbeat"
)

// HeartbeatFactory 是一个示例周期工厂（§5.6、§15.1#12）：按固定周期产生一个心跳任务，
// 幂等键按周期窗口派生，保证同一窗口重复触发只创建一个任务（§5.1）。
type HeartbeatFactory struct {
	interval time.Duration
	channel  string
}

// NewHeartbeatFactory 构造心跳工厂。
func NewHeartbeatFactory(interval time.Duration, channel string) *HeartbeatFactory {
	return &HeartbeatFactory{interval: interval, channel: channel}
}

// Name 实现 factory.Factory。
func (*HeartbeatFactory) Name() string { return "demo.heartbeat" }

// Interval 实现 factory.Factory。
func (f *HeartbeatFactory) Interval() time.Duration { return f.interval }

// Generate 实现 factory.Factory：本窗口内至多一个心跳任务。
func (f *HeartbeatFactory) Generate(_ context.Context, now time.Time) ([]task.Task, error) {
	if f.interval <= 0 {
		return nil, nil
	}
	window := now.Unix() / int64(f.interval.Seconds())
	return []task.Task{task.FuncTask{
		S: task.Spec{
			Type:           DemoType,
			Operator:       DemoOperatorBeat,
			Priority:       50,
			Channel:        f.channel,
			TimeoutMS:      30_000,
			MaxAttempts:    3,
			IdempotencyKey: fmt.Sprintf("demo:heartbeat:%d", window),
			BizGroup:       "demo",
			BizBatchID:     f.Name(),
		},
	}}, nil
}

// RegisterFactories 注册内置工厂（§15.1#12）：具体实现在本包，注册管理在 internal/task。
func RegisterFactories(reg *factory.Registry, heartbeatInterval time.Duration, channel string) error {
	if reg == nil {
		return errors.New("biz: nil factory registry")
	}
	if heartbeatInterval <= 0 {
		// 未启用时不注册，保持装配显式。
		return nil
	}
	return reg.Register(NewHeartbeatFactory(heartbeatInterval, channel))
}

// HeartbeatHandler 是 demo:heartbeat 的处理器（§5.4）。
type HeartbeatHandler struct{}

// Type 实现 worker.Handler。
func (HeartbeatHandler) Type() string { return DemoType }

// Operator 实现 worker.Handler。
func (HeartbeatHandler) Operator() string { return DemoOperatorBeat }

// Handle 实现 worker.Handler。
func (HeartbeatHandler) Handle(_ context.Context, msg *taskv1.TaskMessage) (worker.Result, error) {
	return worker.Result{
		Outcome: taskv1.Outcome_OUTCOME_SUCCEEDED,
		Payload: []byte(fmt.Sprintf(`{"task_id":%d,"attempt":%d}`, msg.GetTaskId(), msg.GetAttempt())),
	}, nil
}

// RegisterHandlers 注册内置 handler。
func RegisterHandlers(reg *worker.HandlerRegistry) error {
	if reg == nil {
		return errors.New("biz: nil handler registry")
	}
	return reg.Register(HeartbeatHandler{})
}

// RegisterPrototypes 注册 codec 可解析的任务原型（§15.1#13）：异步分发/消费的
// TaskMessage 必须在这里登记，codec 才能按签名重建。
func RegisterPrototypes(reg *task.Registry) error {
	if reg == nil {
		return errors.New("biz: nil task registry")
	}
	if _, err := reg.Register(func() proto.Message { return &taskv1.TaskMessage{} }); err != nil {
		return err
	}
	return nil
}
