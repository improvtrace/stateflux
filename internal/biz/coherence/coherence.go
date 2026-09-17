// Package coherence 实现 api/stateflux/coherence/v1 的服务端业务（§15.1#4）：共识快照的
// 进程内持有、分配计算与调度↔执行节点同步。
package coherence

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	coherencev1 "github.com/improvtrace/stateflux/api/stateflux/coherence/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
	"github.com/improvtrace/stateflux/internal/obs"
)

// CoherenceStore 是共识信息的进程内持有者（§15.1#4）：只有调度节点会 Apply 新快照，
// 执行节点只读。revision 单调递增，旧 revision 的 Notify 被丢弃（幂等应用）。
type CoherenceStore struct {
	mu    sync.RWMutex
	state *coherencev1.CoherenceState
}

// NewCoherenceStore 创建空持有者。
func NewCoherenceStore() *CoherenceStore { return &CoherenceStore{} }

// Apply 应用一份新快照：revision 不大于当前值时返回 false（不覆盖）。
func (s *CoherenceStore) Apply(state *coherencev1.CoherenceState) bool {
	if state == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != nil && state.GetRevision() <= s.state.GetRevision() {
		return false
	}
	cp := proto.Clone(state).(*coherencev1.CoherenceState)
	s.state = cp
	return true
}

// Snapshot 返回当前快照副本（可能为 nil）。
func (s *CoherenceStore) Snapshot() *coherencev1.CoherenceState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return nil
	}
	return proto.Clone(s.state).(*coherencev1.CoherenceState)
}

// Revision 返回当前 revision。
func (s *CoherenceStore) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return 0
	}
	return s.state.GetRevision()
}

// ForNode 返回只包含指定节点相关路由的快照；empty 表示返回全量。
func (s *CoherenceStore) ForNode(nodeID string) *coherencev1.CoherenceState {
	full := s.Snapshot()
	if full == nil {
		return nil
	}
	if nodeID == "" {
		return full
	}
	out := &coherencev1.CoherenceState{
		Revision:        full.GetRevision(),
		SchedulerNodeId: full.GetSchedulerNodeId(),
		UpdatedUnixMs:   full.GetUpdatedUnixMs(),
	}
	for _, r := range full.GetRoutes() {
		if r.GetNodeId() == nodeID {
			out.Routes = append(out.Routes, r)
		}
	}
	return out
}

// QueueFor 返回某队列当前的归属节点（执行节点据此判断自己该消费哪些队列）。
func (s *CoherenceStore) QueueFor(queue string) (string, bool) {
	full := s.Snapshot()
	if full == nil {
		return "", false
	}
	for _, r := range full.GetRoutes() {
		if r.GetQueue() == queue {
			return r.GetNodeId(), true
		}
	}
	return "", false
}

// CoherenceServer 实现 coherence/v1.CoherenceService（§15.1#4）：服务端由每个节点承载，
// 调度节点调用对端的 Notify 推送，执行节点也可用 GetCoherence 主动拉取。
type CoherenceServer struct {
	coherencev1.UnimplementedCoherenceServiceServer

	store   *CoherenceStore
	view    cacheview.View
	metrics *obs.Metrics
}

// NewCoherenceServer 构造共识服务端。
func NewCoherenceServer(store *CoherenceStore, view cacheview.View, metrics *obs.Metrics) *CoherenceServer {
	return &CoherenceServer{store: store, view: view, metrics: metrics}
}

// GetCoherence 返回请求方关心的快照。
func (s *CoherenceServer) GetCoherence(_ context.Context, req *coherencev1.GetCoherenceRequest) (*coherencev1.GetCoherenceResponse, error) {
	if req.GetSinceRevision() > 0 && req.GetSinceRevision() >= s.store.Revision() {
		return &coherencev1.GetCoherenceResponse{NotModified: true}, nil
	}
	return &coherencev1.GetCoherenceResponse{State: s.store.ForNode(req.GetNodeId())}, nil
}

// Notify 应用调度节点推送的新快照，并把队列路由物化到 cacheview（best-effort，§15.1#9）。
func (s *CoherenceServer) Notify(ctx context.Context, req *coherencev1.NotifyRequest) (*coherencev1.NotifyResponse, error) {
	applied := s.store.Apply(req.GetState())
	if applied && s.view != nil {
		for _, r := range req.GetState().GetRoutes() {
			_ = s.view.SetQueueRoute(ctx, r.GetQueue(), r.GetNodeId())
		}
	}
	if s.metrics != nil {
		result := "ok"
		if !applied {
			result = "not_modified"
		}
		s.metrics.CoherenceSyncRecord(ctx, result, 1)
		s.metrics.CoherenceRevisionRecord(ctx, s.store.Revision())
	}
	return &coherencev1.NotifyResponse{Accepted: true, AppliedRevision: s.store.Revision()}, nil
}

// Allocator 在调度侧计算「异步队列 → 执行节点」映射（§15.1#4、§15.3#3）。
// 分配是确定性的：候选节点按 ID 排序、队列按名排序后轮转，保证同一输入得到同一映射。
type Allocator struct {
	// SchedulerNodeID 写入快照，便于执行节点识别 owner。
	SchedulerNodeID string
}

// Assign 生成一份新快照。
func (a Allocator) Assign(queues []string, nodes []cluster.Node, revision int64, updatedUnixMs int64) *coherencev1.CoherenceState {
	executors := make([]cluster.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.HasPermission(cluster.NodePermissionOperation) || len(n.Permissions) == 0 {
			executors = append(executors, n)
		}
	}
	if len(executors) == 0 {
		executors = append(executors, nodes...)
	}
	sort.Slice(executors, func(i, j int) bool { return executors[i].ID < executors[j].ID })
	sorted := append([]string(nil), queues...)
	sort.Strings(sorted)

	state := &coherencev1.CoherenceState{
		Revision:        revision,
		SchedulerNodeId: a.SchedulerNodeID,
		UpdatedUnixMs:   updatedUnixMs,
	}
	if len(executors) == 0 {
		return state
	}
	for i, q := range sorted {
		n := executors[i%len(executors)]
		label := ""
		if len(n.Labels) > 0 {
			label = n.Labels[0]
		}
		state.Routes = append(state.Routes, &coherencev1.QueueRoute{
			Queue:  q,
			NodeId: n.ID,
			Vpc:    n.VPC,
			Label:  label,
		})
	}
	return state
}

// CoherencePusher 是调度侧推送客户端（§15.1#4）：把快照 Notify 给执行节点。
type CoherencePusher struct {
	dialer  *rpc.Dialer
	nodes   cluster.Resolver
	timeout time.Duration
}

// NewCoherencePusher 构造推送器。
func NewCoherencePusher(dialer *rpc.Dialer, nodes cluster.Resolver, timeout time.Duration) *CoherencePusher {
	return &CoherencePusher{dialer: dialer, nodes: nodes, timeout: timeout}
}

// Push 向单个节点推送快照。
func (p *CoherencePusher) Push(ctx context.Context, state *coherencev1.CoherenceState, targetNodeID string) error {
	if p.dialer == nil || p.nodes == nil {
		return errors.New("biz: coherence pusher missing dialer or cluster view")
	}
	node, ok := p.nodes.Node(targetNodeID)
	if !ok || node.Address == "" {
		return fmt.Errorf("biz: unknown coherence target %q", targetNodeID)
	}
	conn, err := p.dialer.Conn(node.Address)
	if err != nil {
		return err
	}
	callCtx := ctx
	if p.timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	_, err = coherencev1.NewCoherenceServiceClient(conn).Notify(callCtx, &coherencev1.NotifyRequest{
		State:        state,
		TargetNodeId: targetNodeID,
	})
	return err
}

// PushAll 向全部候选节点推送，返回聚合错误。
func (p *CoherencePusher) PushAll(ctx context.Context, state *coherencev1.CoherenceState, nodeIDs []string) error {
	var errs []error
	for _, id := range nodeIDs {
		if err := p.Push(ctx, state, id); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}
