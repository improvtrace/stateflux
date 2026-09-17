// Package forward 执行节点间的 RPC 转发（§15.1#6）：把「目标方法 + 序列化请求」投递到
// 目标节点在本机处理，或按 target_node_id 继续中转。它只做中转，不产生调度决策，也不
// 改变任务归属（§15.3#4）。
//
// 环路保护：请求携带 visited_nodes 与 ttl；接收节点发现自己在 visited 中、或 ttl <= 0
// 时拒绝继续转发，避免节点互指形成死循环。
package forward

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/grpc"

	forwardv1 "github.com/improvtrace/stateflux/api/stateflux/forward/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
)

// ErrNoHandler 表示目标方法在本节点没有注册 handler。
var ErrNoHandler = errors.New("forward: no local handler for method")

// ErrLoop 表示检测到转发环路（目标已在 visited 中）。
var ErrLoop = errors.New("forward: forwarding loop detected")

// Handler 处理一个已转发到本节点的方法调用。
type Handler func(ctx context.Context, payload []byte, headers map[string]string) ([]byte, error)

// Registry 是「RPC 方法全名 → 本地 handler」的注册表。biz 在装配期把 DispatchService
// 等方法注册进来（例如 /dispatch.v1.DispatchService/Dispatch），使转发请求能在本节点落地。
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}}
}

// Register 注册 handler；空方法名、nil 或重名返回错误。
func (r *Registry) Register(method string, h Handler) error {
	if method == "" {
		return errors.New("forward: empty method")
	}
	if h == nil {
		return errors.New("forward: nil handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[method]; ok {
		return fmt.Errorf("forward: duplicate method %q", method)
	}
	r.handlers[method] = h
	return nil
}

// Handle 调用本地 handler。
func (r *Registry) Handle(ctx context.Context, method string, payload []byte, headers map[string]string) ([]byte, error) {
	r.mu.RLock()
	h, ok := r.handlers[method]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoHandler, method)
	}
	return h(ctx, payload, headers)
}

// Methods 返回已注册方法（稳定排序）。
func (r *Registry) Methods() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.handlers))
	for m := range r.handlers {
		out = append(out, m)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

// Router 是继续中转所需的最小依赖（由 Forwarder 实现）。
type Router interface {
	Forward(ctx context.Context, targetNodeID, method string, payload []byte, headers map[string]string, ttl int32) ([]byte, error)
}

// Server 实现 forwardv1.ForwardServiceServer：先本地处理，必要时继续中转。
type Server struct {
	forwardv1.UnimplementedForwardServiceServer

	registry *Registry
	router   Router
	self     string
	maxHops  int
}

// NewServer 构造转发服务端；self 是本节点 ID，maxHops 是允许的最大转发跳数。
func NewServer(registry *Registry, router Router, self string, maxHops int) *Server {
	return &Server{registry: registry, router: router, self: self, maxHops: maxHops}
}

// Forward 实现 forwardv1.ForwardService。
func (s *Server) Forward(ctx context.Context, req *forwardv1.ForwardRequest) (*forwardv1.ForwardResponse, error) {
	visited := req.GetVisitedNodes()
	if containsString(visited, s.self) {
		return nil, fmt.Errorf("%w: %s already visited", ErrLoop, s.self)
	}
	if s.registry != nil {
		if _, ok := s.lookup(req.GetMethod()); ok {
			payload, err := s.registry.Handle(ctx, req.GetMethod(), req.GetPayload(), req.GetHeaders())
			if err != nil {
				return nil, err
			}
			return &forwardv1.ForwardResponse{Payload: payload, ServedBy: s.self}, nil
		}
	}
	target := req.GetTargetNodeId()
	if target != "" && target != s.self {
		if s.router == nil {
			return nil, fmt.Errorf("forward: cannot relay %q: no router", req.GetMethod())
		}
		if req.GetTtl() <= 0 {
			return nil, fmt.Errorf("forward: ttl exhausted for %q", req.GetMethod())
		}
		headers := cloneHeaders(req.GetHeaders())
		headers["forward.visited"] = joinStrings(append(visited, s.self))
		payload, err := s.router.Forward(ctx, target, req.GetMethod(), req.GetPayload(), headers, req.GetTtl()-1)
		if err != nil {
			return nil, err
		}
		return &forwardv1.ForwardResponse{Payload: payload, ServedBy: target}, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrNoHandler, req.GetMethod())
}

func (s *Server) lookup(method string) (Handler, bool) {
	s.registry.mu.RLock()
	defer s.registry.mu.RUnlock()
	h, ok := s.registry.handlers[method]
	return h, ok
}

// Dialer 是 Forwarder 依赖的 gRPC 连接池接口（rpc.Dialer 实现）。
type Dialer interface {
	Conn(address string) (*grpc.ClientConn, error)
}

// Forwarder 是 ForwardService 的客户端：按节点解析地址并发送，必要时由服务端继续中转。
type Forwarder struct {
	dialer  Dialer
	nodes   cluster.Resolver
	self    string
	maxHops int
}

// NewForwarder 构造转发客户端。
func NewForwarder(dialer Dialer, nodes cluster.Resolver, self string, maxHops int) *Forwarder {
	if maxHops <= 0 {
		maxHops = 3
	}
	return &Forwarder{dialer: dialer, nodes: nodes, self: self, maxHops: maxHops}
}

// Forward 把方法调用转发到目标节点；ttl <= 0 时使用构造时的 maxHops。
func (f *Forwarder) Forward(ctx context.Context, targetNodeID, method string, payload []byte, headers map[string]string, ttl int32) ([]byte, error) {
	if targetNodeID == "" {
		return nil, errors.New("forward: empty target node")
	}
	if f.nodes == nil {
		return nil, fmt.Errorf("forward: no cluster view to resolve %q", targetNodeID)
	}
	node, ok := f.nodes.Node(targetNodeID)
	if !ok || node.Address == "" {
		return nil, fmt.Errorf("forward: unknown target node %q", targetNodeID)
	}
	if ttl <= 0 {
		ttl = int32(f.maxHops)
	}
	conn, err := f.dialer.Conn(node.Address)
	if err != nil {
		return nil, err
	}
	resp, err := forwardv1.NewForwardServiceClient(conn).Forward(ctx, &forwardv1.ForwardRequest{
		Method:       method,
		TargetNodeId: targetNodeID,
		Payload:      payload,
		Headers:      cloneHeaders(headers),
		VisitedNodes: []string{f.self},
		Ttl:          ttl,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetError() != "" {
		return nil, errors.New(resp.GetError())
	}
	return resp.GetPayload(), nil
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func joinStrings(list []string) string {
	out := ""
	for i, s := range list {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

func cloneHeaders(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ forwardv1.ForwardServiceServer = (*Server)(nil)
