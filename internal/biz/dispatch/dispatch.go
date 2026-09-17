// Package dispatch 实现 api/stateflux/dispatch/v1 的服务端业务（§15.1#5）：对外分发入口与节点间转发。
package dispatch

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	dispatchv1 "github.com/improvtrace/stateflux/api/stateflux/dispatch/v1"
	bizcoherence "github.com/improvtrace/stateflux/internal/biz/coherence"
	"github.com/improvtrace/stateflux/internal/biz/forward"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/obs"
	taskdispatch "github.com/improvtrace/stateflux/internal/task/dispatch"
)

// DispatchMethod 是 DispatchService.Dispatch 的 RPC 方法全名（转发注册键，§15.1#6）。
const DispatchMethod = "/dispatch.v1.DispatchService/Dispatch"

// DispatchServer 实现 dispatch/v1.DispatchService（§15.1#5）：提供对外分发入口，
// 支持 at_least_once / at_most_once / exactly_once 三种语义、redis 队列与 rpc 两种投递，
// 并在目标非本节点时经 internal/biz/forward 做节点间转发。
type DispatchServer struct {
	dispatchv1.UnimplementedDispatchServiceServer

	dispatcher *taskdispatch.Dispatcher
	nodes      *cluster.Cache
	forwarder  *forward.Forwarder
	self       string
	cfg        config.Dispatch
	metrics    *obs.Metrics
}

// DispatchServerOptions 是装配参数。
type DispatchServerOptions struct {
	Dispatcher *taskdispatch.Dispatcher
	Nodes      *cluster.Cache
	Forwarder  *forward.Forwarder
	Self       string
	Config     config.Dispatch
	Metrics    *obs.Metrics
}

// NewDispatchServer 构造分发服务端。
func NewDispatchServer(opts DispatchServerOptions) *DispatchServer {
	return &DispatchServer{
		dispatcher: opts.Dispatcher,
		nodes:      opts.Nodes,
		forwarder:  opts.Forwarder,
		self:       opts.Self,
		cfg:        opts.Config,
		metrics:    opts.Metrics,
	}
}

// Dispatch 实现 dispatch/v1.DispatchService.Dispatch。
func (s *DispatchServer) Dispatch(ctx context.Context, req *dispatchv1.DispatchRequest) (*dispatchv1.DispatchResponse, error) {
	if req.GetTask() == nil {
		return nil, errors.New("biz: dispatch requires a task message")
	}
	target := req.GetTargetNodeId()
	if target == "" {
		node, err := s.dispatcher.ResolveTarget(req.GetTask())
		if err != nil {
			return nil, err
		}
		target = node.ID
	}
	// 节点间转发：目标非本节点且调用方允许转发时，把整个 DispatchRequest 交给目标节点处理。
	if s.shouldForward(req, target) {
		return s.forward(ctx, req, target)
	}
	return s.deliverLocal(ctx, req, target)
}

func (s *DispatchServer) shouldForward(req *dispatchv1.DispatchRequest, target string) bool {
	return s.cfg.Forward && req.GetAllowForward() && s.forwarder != nil && target != "" && target != s.self
}

// forward 经 ForwardService 把请求转给目标节点，并记录转发指标。
func (s *DispatchServer) forward(ctx context.Context, req *dispatchv1.DispatchRequest, target string) (*dispatchv1.DispatchResponse, error) {
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	visited := append(append([]string(nil), req.GetVisitedNodes()...), s.self)
	headers := map[string]string{
		"dispatch.origin":  s.self,
		"dispatch.target":  target,
		"dispatch.visited": joinVisit(visited),
	}
	out, err := s.forwarder.Forward(ctx, target, DispatchMethod, payload, headers, int32(s.cfg.MaxHops))
	if err != nil {
		return nil, err
	}
	resp := &dispatchv1.DispatchResponse{}
	if err := proto.Unmarshal(out, resp); err != nil {
		return nil, fmt.Errorf("biz: decode forwarded dispatch response: %w", err)
	}
	resp.ForwardedBy = s.self
	if s.metrics != nil {
		s.metrics.DispatchForwardedRecord(ctx, "sent", 1)
	}
	return resp, nil
}

// deliverLocal 在本节点完成投递编排。
func (s *DispatchServer) deliverLocal(ctx context.Context, req *dispatchv1.DispatchRequest, target string) (*dispatchv1.DispatchResponse, error) {
	node := cluster.Node{ID: target}
	if s.nodes != nil {
		if n, ok := s.nodes.Node(target); ok {
			node = n
		}
	}
	res, err := s.dispatcher.Deliver(ctx, taskdispatch.Request{
		Queue:         req.GetQueue(),
		Message:       req.GetTask(),
		Delivery:      deliveryOf(req.GetDelivery()),
		Semantics:     semanticsOf(req.GetSemantics()),
		Target:        node,
		DedupeKey:     req.GetDedupeKey(),
		CorrelationID: req.GetTask().GetIdempotencyKey(),
	})
	if err != nil {
		if s.metrics != nil {
			s.metrics.DispatchRecord(ctx, string(semanticsOf(req.GetSemantics())), string(deliveryOf(req.GetDelivery())), "error", 1)
		}
		return nil, err
	}
	if s.metrics != nil {
		result := "accepted"
		if res.Duplicate {
			result = "duplicate"
		}
		s.metrics.DispatchRecord(ctx, string(semanticsOf(req.GetSemantics())), string(deliveryOf(req.GetDelivery())), result, 1)
	}
	return &dispatchv1.DispatchResponse{
		TaskId:       res.TaskID,
		TargetNodeId: res.TargetNodeID,
		Accepted:     res.Accepted,
		Duplicate:    res.Duplicate,
		Message:      res.Message,
	}, nil
}

// HandleForwarded 是 forward.Registry 的 handler：接收已转发到本节点的 DispatchRequest，
// 强制本地投递（不再转发）以避免环路（§15.3#4）。
func (s *DispatchServer) HandleForwarded(ctx context.Context, payload []byte, headers map[string]string) ([]byte, error) {
	req := &dispatchv1.DispatchRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, err
	}
	target := req.GetTargetNodeId()
	if target == "" {
		target = s.self
	}
	if s.metrics != nil {
		s.metrics.DispatchForwardedRecord(ctx, "received", 1)
	}
	resp, err := s.deliverLocal(ctx, req, target)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp)
}

// LocalQueueFor 返回共识视图给出的队列归属（供执行节点决定订阅哪些队列）。
func (s *DispatchServer) LocalQueueFor(state *bizcoherence.CoherenceStore, queue string) (string, bool) {
	if state == nil {
		return "", false
	}
	return state.QueueFor(queue)
}

func semanticsOf(v dispatchv1.Semantics) config.Semantics {
	switch v {
	case dispatchv1.Semantics_AT_MOST_ONCE:
		return config.AtMostOnce
	case dispatchv1.Semantics_EXACTLY_ONCE:
		return config.ExactlyOnce
	default:
		return config.AtLeastOnce
	}
}

func deliveryOf(v dispatchv1.Delivery) config.Delivery {
	if v == dispatchv1.Delivery_DELIVERY_SYNC_RPC {
		return config.DeliverySyncRPC
	}
	return config.DeliveryRedisQueue
}

func joinVisit(list []string) string {
	out := ""
	for i, s := range list {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

// 编译期：确保 DispatchServer 能作为转发 handler 使用（签名与 forward.Handler 一致）。
var _ = func(s *DispatchServer) forward.Handler { return s.HandleForwarded }
