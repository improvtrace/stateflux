// Package cluster 提供 ClusterView —— 唯一的集群事实来源（§1.2.6「集群信息只读不治」）。
// stateflux 不实现选举：进程通过 ClusterView 周期性获取节点信息（成员、角色、能力标签）
// 与当前调度节点位置，据此决定本实例启用哪些角色（§2.1）。
package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Role 角色常量（§2.1）：API 与 Executor 所有实例常驻；Scheduler/Collector/Reconciler/Factory
// 仅在外部选举指向本实例时运行。
const (
	RoleAPI        = "api"
	RoleExecutor   = "executor"
	RoleScheduler  = "scheduler"
	RoleCollector  = "collector"
	RoleReconciler = "reconciler"
	RoleFactory    = "factory"
)

// Node 集群节点信息。
type Node struct {
	ID           string
	Address      string // gRPC 服务地址（Execute/Collect）
	Roles        []string
	Capabilities []string
}

// HasRole 报告节点是否承担指定角色。
func (n Node) HasRole(role string) bool {
	for _, r := range n.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasCapability 报告节点是否具备指定能力标签。
func (n Node) HasCapability(cap string) bool {
	for _, c := range n.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// View 集群视图。实现必须并发安全，并周期性刷新（static 立即、external 按 RefreshInterval）。
type View interface {
	// Refresh 拉取一次最新集群信息。
	Refresh(ctx context.Context) error
	// Nodes 当前全部节点。
	Nodes() []Node
	// SchedulerID 当前调度节点 ID；空表示集群暂无调度节点。
	SchedulerID() string
	// Self 本节点信息（View 实现自行携带）。
	Self() Node
	// ExecutorNodes 按 roles 过滤出执行节点（同步任务选节点用，§5.3）。
	ExecutorNodes() []Node
}

// StaticConfig static 单机/开发模式配置（§2.1）：把全部角色赋给本进程，一个进程闭环。
type StaticConfig struct {
	NodeID       string
	Address      string
	Capabilities []string
	// Roles 覆盖默认全角色（嵌入形态可只挂 executor，§1.2.8）。
	Roles []string
}

// NewStatic 构造 static 视图。默认角色 = 全角色（api/executor/scheduler/collector/reconciler/factory）。
func NewStatic(cfg StaticConfig) *Static {
	if cfg.NodeID == "" {
		cfg.NodeID = "static-0"
	}
	roles := cfg.Roles
	if len(roles) == 0 {
		roles = []string{RoleAPI, RoleExecutor, RoleScheduler, RoleCollector, RoleReconciler, RoleFactory}
	}
	self := Node{ID: cfg.NodeID, Address: cfg.Address, Roles: roles, Capabilities: cfg.Capabilities}
	return &Static{self: self}
}

// Static 全部角色赋给单进程的实现（单机/开发模式）。
type Static struct {
	self Node
}

// Refresh 无操作（static 拓扑固定）。
func (s *Static) Refresh(context.Context) error { return nil }

// Nodes 返回本进程自身。
func (s *Static) Nodes() []Node { return []Node{s.self} }

// SchedulerID 恒为自身（static 模式下本进程即调度节点）。
func (s *Static) SchedulerID() string { return s.self.ID }

// Self 返回本节点。
func (s *Static) Self() Node { return s.self }

// ExecutorNodes 返回自身（含 executor 角色）。
func (s *Static) ExecutorNodes() []Node { return []Node{s.self} }

// RPCClient 通过外部选举/成员服务的 GetClusterInfo RPC 获取集群视图（§7 cluster.proto）。
// 客户端缓存上一次结果，Refresh 周期性调用；单次失败保留旧值（集群信息是易失实时视图，
// 短暂不可用不应让角色停摆）。
type RPCClient struct {
	endpoint string
	insecure bool
	interval time.Duration
	nodeID   string
	address  string
	caps     []string

	mu        sync.RWMutex
	nodes     []Node
	scheduler string

	// conn 复用 gRPC 连接（懒建）。
	connMu sync.Mutex
	conn   *grpcClientConn
}

// NewRPCClient 构造外部集群视图客户端。
func NewRPCClient(endpoint, nodeID, address string, caps []string, refresh time.Duration, insecure bool) *RPCClient {
	if refresh <= 0 {
		refresh = 5 * time.Second
	}
	return &RPCClient{
		endpoint: endpoint,
		insecure: insecure,
		interval: refresh,
		nodeID:   nodeID,
		address:  address,
		caps:     caps,
	}
}

// Interval 返回刷新周期。
func (c *RPCClient) Interval() time.Duration { return c.interval }

// Refresh 调用 GetClusterInfo 并更新缓存。失败时返回错误但保留旧值。
func (c *RPCClient) Refresh(ctx context.Context) error {
	resp, err := c.fetch(ctx)
	if err != nil {
		return fmt.Errorf("cluster: get cluster info: %w", err)
	}
	c.mu.Lock()
	c.nodes = resp.nodes
	c.scheduler = resp.schedulerID
	c.mu.Unlock()
	return nil
}

// Nodes 返回缓存节点列表。
func (c *RPCClient) Nodes() []Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]Node(nil), c.nodes...)
}

// SchedulerID 返回缓存的调度节点 ID。
func (c *RPCClient) SchedulerID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.scheduler
}

// Self 返回本节点（由配置注入，外部选举服务不必回显自身）。
func (c *RPCClient) Self() Node {
	return Node{ID: c.nodeID, Address: c.address, Capabilities: c.caps}
}

// ExecutorNodes 过滤 executor 角色。
func (c *RPCClient) ExecutorNodes() []Node {
	var out []Node
	for _, n := range c.Nodes() {
		if n.HasRole(RoleExecutor) {
			out = append(out, n)
		}
	}
	return out
}
