package task

import (
	"context"
	"errors"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	bizcoherence "github.com/improvtrace/stateflux/internal/biz/coherence"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// ResultPublisher 是 worker.ResultSink 的默认实现（§5.4）：把 ResultEvent 经 EventBus
// 送到调度节点的结果通道（默认 gRPC 双向 ResultStream），并顺带更新异步执行视图。
type ResultPublisher struct {
	bus         *eventbus.EventBus
	channelName string
	nodes       cluster.Resolver
	view        cacheview.View
	timeout     time.Duration
}

// ResultPublisherOptions 是装配参数。
type ResultPublisherOptions struct {
	Bus         *eventbus.EventBus
	ChannelName string
	Nodes       cluster.Resolver
	View        cacheview.View
	Timeout     time.Duration
}

// NewResultPublisher 构造结果发布器。
func NewResultPublisher(opts ResultPublisherOptions) *ResultPublisher {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ResultPublisher{
		bus:         opts.Bus,
		channelName: opts.ChannelName,
		nodes:       opts.Nodes,
		view:        opts.View,
		timeout:     timeout,
	}
}

// Publish 实现 worker.ResultSink。
func (p *ResultPublisher) Publish(ctx context.Context, ev *taskv1.ResultEvent) error {
	if p.bus == nil {
		return errors.New("biz: result publisher requires eventbus")
	}
	if p.channelName == "" {
		p.channelName = ChannelStreamName
	}
	payload, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	env := channel.Envelope{
		Topic:   eventbus.ResultTopic(),
		Key:     strconv.FormatInt(ev.GetTaskId(), 10),
		Attempt: ev.GetAttempt(),
		Payload: payload,
	}
	if p.nodes != nil {
		if sched, ok := p.nodes.Node(bizcoherence.SchedulerNodeID(p.nodes)); ok {
			env.Target = sched.Address
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if _, err := p.bus.Call(callCtx, env, eventbus.WithChannel(p.channelName)); err != nil {
		return err
	}
	p.updateView(ctx, ev)
	return nil
}

// updateView 更新任务异步执行视图与在途计数（best-effort，§15.1#9）。
func (p *ResultPublisher) updateView(ctx context.Context, ev *taskv1.ResultEvent) {
	if p.view == nil {
		return
	}
	state := cacheview.StateSucceeded
	if ev.GetOutcome() != taskv1.Outcome_OUTCOME_SUCCEEDED {
		state = cacheview.StateFailed
	}
	_ = p.view.SetTaskState(ctx, cacheview.TaskState{
		TaskID:  ev.GetTaskId(),
		Attempt: ev.GetAttempt(),
		State:   state,
		NodeID:  ev.GetSource(),
		Error:   ev.GetError(),
	})
	if ev.GetSource() != "" {
		_, _ = p.view.DecrInflight(ctx, ev.GetSource())
	}
}

// ChannelStreamName 是结果通道的逻辑名（装配时映射到 rpc-stream）。
const ChannelStreamName = "stream"
