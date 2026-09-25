// Package dispatch 是调度侧分发编排（§5.3、§8、§15.1#5）：把已认领任务按任务行的
// 逻辑 channel 解析 eventbus 实现、按 vpc/node/label/hash_bucket 选执行节点，并按
// 投递语义与投递形态发送。它不写 PG 终态、不解释业务——终态只在 Collector 事务内发生。
//
// 与确认条款的对应（§15.1#5）：
//   - 语义：at_least_once（允许重试）/ at_most_once（失败即放弃）/ exactly_once（入口去重）；
//   - 形态：异步 redis queue（单向，不等结果）/ 同步 rpc（请求-应答，立即拿到 ResultEvent）；
//   - 节点间转发由服务层（internal/biz 的 DispatchService）经 internal/biz/forward 完成，
//     本包只负责「把任务交给目标节点」这一步，转发不改变归属。
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/codec"
)

// ErrNoTargetNode 表示当前集群视图没有满足任务约束的执行节点：调度侧应保留任务
// 不发送（等视图更新或由 R1 收敛），而不是发到任意节点（§5.3）。
var ErrNoTargetNode = errors.New("task/dispatch: no executor node matches task constraints")

// Forwarder 是节点间转发的最小依赖面（internal/biz/forward 实现，接口在此声明以避免
// dispatch → forward → biz → dispatch 的环）。方法名与 forward.Forwarder 保持一致。
type Forwarder interface {
	Forward(ctx context.Context, targetNodeID, method string, payload []byte, headers map[string]string, ttl int32) ([]byte, error)
}

// Request 是一次分发请求（调度侧内部表示）。
type Request struct {
	// Queue 是逻辑队列/channel 名；空则取 Message.Channel。
	Queue string
	// Message 是任务信封（不可变）。
	Message *taskv1.TaskMessage
	// Delivery 是投递形态；空取默认。
	Delivery task.Delivery
	// Semantics 是投递语义；空取默认。
	Semantics task.Semantics
	// Target 是显式目标节点；零值时由约束选择。
	Target cluster.Node
	// DedupeKey 是 exactly_once 的入口去重键；空则回落到 idempotency_key / task_id。
	DedupeKey string
	// CorrelationID 端到端配对（§6.3）。
	CorrelationID string
}

// Result 是一次分发的结果。Accepted 仅表示已受理/已尝试投递，不表示任务已执行。
type Result struct {
	TaskID       int64
	TargetNodeID string
	Accepted     bool
	Duplicate    bool
	// Retryable 由语义推导：at_most_once 为 false（失败即放弃）。
	Retryable bool
	Message   string
	// Result 仅同步 rpc 投递且对端返回结果时非空。
	Result *taskv1.ResultEvent
}

// Options 是 Dispatcher 的装配参数。
type Options struct {
	Bus       *eventbus.EventBus
	Nodes     cluster.ClusterCacheView
	View      cacheview.View
	Codec     *codec.Codec
	Self      string
	Config    config.Dispatch
	Forwarder Forwarder
	Metrics   *obs.Metrics
}

// Dispatcher 执行单次本地分发编排。
type Dispatcher struct {
	bus       *eventbus.EventBus
	nodes     cluster.ClusterCacheView
	view      cacheview.View
	codec     *codec.Codec
	self      string
	cfg       config.Dispatch
	forwarder Forwarder
	metrics   *obs.Metrics
}

// New 构造分发器；bus/codec/view 为必需依赖，缺失时 panic（装配期尽早失败）。
func New(opts Options) *Dispatcher {
	if opts.Bus == nil {
		panic("task/dispatch: nil eventbus")
	}
	if opts.View == nil {
		panic("task/dispatch: nil cacheview")
	}
	if opts.Codec == nil {
		panic("task/dispatch: nil codec")
	}
	return &Dispatcher{
		bus:       opts.Bus,
		nodes:     opts.Nodes,
		view:      opts.View,
		codec:     opts.Codec,
		self:      opts.Self,
		cfg:       opts.Config,
		forwarder: opts.Forwarder,
		metrics:   opts.Metrics,
	}
}

// Deliver 执行一次分发。
func (d *Dispatcher) Deliver(ctx context.Context, req Request) (*Result, error) {
	if req.Message == nil {
		return nil, errors.New("task/dispatch: nil task message")
	}
	queue := req.Queue
	if queue == "" {
		queue = req.Message.GetChannel()
	}
	if queue == "" {
		return nil, errors.New("task/dispatch: empty queue/channel")
	}
	delivery := req.Delivery
	if delivery == "" {
		delivery = d.cfg.DefaultDelivery
	}
	semantics := req.Semantics
	if semantics == "" {
		semantics = d.cfg.DefaultSemantics
	}
	res := &Result{
		TaskID:    req.Message.GetTaskId(),
		Retryable: semantics != task.AtMostOnce,
	}

	// exactly_once：入口去重。Redis 不可用时返回错误——绝不静默放行重复投递（§15.3#2）。
	if semantics == task.ExactlyOnce {
		key := req.DedupeKey
		if key == "" {
			key = req.Message.GetIdempotencyKey()
		}
		if key == "" {
			key = strconv.FormatInt(req.Message.GetTaskId(), 10)
		}
		first, err := d.view.ClaimDedupe(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("task/dispatch: dedupe claim: %w", err)
		}
		if !first {
			res.Accepted = true
			res.Duplicate = true
			res.Message = "exactly_once dedupe hit"
			return res, nil
		}
	}

	target, err := d.resolveTarget(req)
	if err != nil {
		return nil, err
	}
	res.TargetNodeID = target.ID

	env, err := d.buildEnvelope(queue, delivery, req, target)
	if err != nil {
		return nil, err
	}

	d.recordSend(ctx, queue, string(delivery))
	resp, err := d.bus.Call(ctx, env, eventbus.WithChannel(queue))
	if err != nil {
		res.Message = err.Error()
		if d.metrics != nil {
			d.metrics.ErrorRecord(ctx, queue, "deliver")
		}
		if semantics == task.AtMostOnce {
			// 至多一次：放弃重试，返回「已尝试」而非错误——由调用方决定是否记录。
			return res, nil
		}
		return res, err
	}
	res.Accepted = true

	if delivery == task.DeliverySyncRPC && len(resp.Payload) > 0 {
		ev := &taskv1.ResultEvent{}
		if uerr := proto.Unmarshal(resp.Payload, ev); uerr == nil && ev.GetTaskId() != 0 {
			res.Result = ev
		}
	}

	// 异步执行视图（best-effort 提示，失败不影响正确性，§15.1#9）。
	if delivery == task.DeliveryRedisQueue {
		_ = d.view.SetTaskState(ctx, cacheview.TaskState{
			TaskID:  req.Message.GetTaskId(),
			Attempt: req.Message.GetAttempt(),
			State:   cacheview.StateDispatched,
			NodeID:  target.ID,
			Queue:   queue,
		})
		_, _ = d.view.IncrInflight(ctx, target.ID)
		if err := d.view.SetQueueRoute(ctx, queue, target.ID); err != nil && d.metrics != nil {
			d.metrics.ErrorRecord(ctx, queue, "cacheview")
		}
	}
	return res, nil
}

// resolveTarget 解析目标执行节点：显式 target 优先，否则按约束选择。
func (d *Dispatcher) resolveTarget(req Request) (cluster.Node, error) {
	if req.Target.ID != "" {
		return req.Target, nil
	}
	return d.ResolveTarget(req.Message)
}

// ResolveTarget 按任务约束选择目标执行节点（服务层在决定「本地投递 or 节点间转发」
// 前调用它）。无集群视图时退化为本节点（单机/测试装配）。
func (d *Dispatcher) ResolveTarget(msg *taskv1.TaskMessage) (cluster.Node, error) {
	if d.nodes != nil {
		if n, ok := d.nodes.Select(msg.GetNode(), msg.GetVpc(), msg.GetLabel(), int(msg.GetHashBucket())); ok {
			return n, nil
		}
		return cluster.Node{}, ErrNoTargetNode
	}
	if d.self == "" {
		return cluster.Node{}, ErrNoTargetNode
	}
	return cluster.Node{ID: d.self}, nil
}

// buildEnvelope 依据投递形态编码载荷：同步走 protobuf 直传，异步走 machinery 风格的
// 签名化 codec（§15.1#13）。
func (d *Dispatcher) buildEnvelope(queue string, delivery task.Delivery, req Request, target cluster.Node) (channel.Envelope, error) {
	var (
		payload []byte
		err     error
	)
	switch delivery {
	case task.DeliverySyncRPC:
		payload, err = proto.Marshal(req.Message)
	default:
		payload, err = d.codec.Encode(req.Message)
	}
	if err != nil {
		return channel.Envelope{}, err
	}
	return channel.Envelope{
		Topic:         channel.Topic(queue),
		Key:           strconv.FormatInt(req.Message.GetTaskId(), 10),
		Attempt:       req.Message.GetAttempt(),
		CorrelationID: req.CorrelationID,
		Target:        target.Address,
		Payload:       payload,
	}, nil
}

func (d *Dispatcher) recordSend(ctx context.Context, queue, duplex string) {
	if d.metrics != nil {
		d.metrics.SendRecord(ctx, queue, duplex)
	}
}
