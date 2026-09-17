// Package cluster 提供集群视图（ClusterView）外部接口、RPC 客户端与 static 单机
// 实现（§2.1、§8、§15.1#3）。集群信息只读不治：选举由外部系统负责，stateflux 仅消费。
//
// 连接信息来自 config.Cluster（§15.1#3），仅一个 DSN 字段：local://、http(s)://、grpc://，
// 连接与调用参数（timeout / connect_timeout / interval）经 URL query 声明。
// 本包只做「拉取 + 快照 + 选择」，不做任何调度决策之外的裁决：路由决策仍在调度侧
// （§1.2.7），本包的选择辅助函数只是把 vpc/label/node/bucket 过滤与哈希分桶集中一处。
package cluster

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/improvtrace/stateflux/internal/config"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
)

// Role 是节点角色（外置集群的定义，§2.1/§15.1#3）：仅有 primary 与 standby。
type Role = string

const (
	// RolePrimary 主节点：具有数据库读写能力。
	RolePrimary Role = "primary"
	// RoleStandby 备节点：具备数据库读权限。
	RoleStandby Role = "standby"
)

// NodePermission 是节点服务权限（stateflux 侧定义，随外置集群节点信息下发）：
// 声明本节点启用哪些服务模块。
type NodePermission string

const (
	// NodePermissionOperation 业务操作：节点启用执行侧 RPC（api/stateflux/worker）。
	NodePermissionOperation NodePermission = "operation"
	// NodePermissionExecute 任务执行：节点启用通用执行操作（api/stateflux/task）。
	// 不变量：所有节点都必须启用该权限。
	NodePermissionExecute NodePermission = "execute"
	// NodePermissionSchedule 调度能力：节点启用调度能力（晋升/认领/分发）。
	NodePermissionSchedule NodePermission = "schedule"
	// NodePermissionCollect 归集能力：节点启用归集模块，归集结果。
	NodePermissionCollect NodePermission = "collect"
)

// permissionsOf 推导节点服务权限：primary（库读写）具备全部权限；standby（库只读）
// 仅保留业务操作与任务执行——调度与归集需写库，不授予只读节点。
// 不变量：无论来源如何， NodePermissionExecute 恒在。
func permissionsOf(roles []string) []NodePermission {
	primary := false
	for _, r := range roles {
		if Role(r) == RolePrimary {
			primary = true
			break
		}
	}
	var perms []NodePermission
	if primary {
		perms = []NodePermission{NodePermissionOperation, NodePermissionExecute, NodePermissionSchedule, NodePermissionCollect}
	} else {
		perms = []NodePermission{NodePermissionOperation, NodePermissionExecute}
	}
	return perms
}

// Node 是集群视图中的一个节点（§15.1#3）：node_id + vpc + labels 是拉取的核心三要素，
// address 用于通信与路由；online/is_leader 描述节点实时状态。
type Node struct {
	// ID 节点唯一标识（集群内稳定）。
	ID string
	// Address 对外可达的 gRPC 地址（host:port）。
	Address string
	// VPC 目标网络域（调度目标节点属性）。
	VPC string
	// Labels 节点匹配标签（自由文本，框架不解释语义）。
	Labels []string
	// Roles 节点角色（外置集群的定义）：仅 primary（库读写）/ standby（库只读）。
	Roles []string
	// Permissions 节点服务权限：由角色推导，恒含 NodePermissionExecute（不变量）。
	Permissions []NodePermission
	// Online 节点是否在线。
	Online bool
	// IsLeader 节点是否为 leader 节点。
	IsLeader bool
}

// HasRole 判断节点是否承担某角色（primary / standby）。
func (n Node) HasRole(r Role) bool {
	for _, role := range n.Roles {
		if strings.EqualFold(role, string(r)) {
			return true
		}
	}
	return false
}

// HasPermission 判断节点是否启用某服务权限。
func (n Node) HasPermission(p NodePermission) bool {
	for _, p2 := range n.Permissions {
		if p2 == p {
			return true
		}
	}
	return false
}

// HasLabel 判断节点是否携带某匹配标签。
func (n Node) HasLabel(label string) bool {
	for _, l := range n.Labels {
		if l == label {
			return true
		}
	}
	return false
}

// Info 是一次集群视图快照。
type Info struct {
	// Nodes 当前全部节点。
	Nodes []Node
	// NodeID 当前节点 ID（集群视图上报的本实例身份；视图未提供时为空）。
	NodeID string
	// SchedulerNodeID 当前 leader 节点 ID；空表示集群暂无 leader（§2.1）。
	SchedulerNodeID string
}

// Scheduler 返回当前调度节点。
func (i Info) Scheduler() (Node, bool) {
	return i.Node(i.SchedulerNodeID)
}

// Node 按 ID 查节点。
func (i Info) Node(id string) (Node, bool) {
	for _, n := range i.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// View 是集群视图的只读客户端（§15.1#3）：每次调用拉取一份最新快照。
type View interface {
	Get(ctx context.Context) (Info, error)
}

// Select 在候选节点中按调度目标节点属性选择执行节点（§5.2/§5.3）：
// 依次过滤 node/vpc/label，再按 hash_bucket 做稳定分桶（bucket == 0 表示不限）。
// 返回 false 表示当前视图没有满足约束的节点——调度侧据此保留任务不发送。
func Select(nodes []Node, nodeID, vpc, label string, bucket int) (Node, bool) {
	candidates := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if nodeID != "" && n.ID != nodeID {
			continue
		}
		if vpc != "" && n.VPC != vpc {
			continue
		}
		if label != "" && !n.HasLabel(label) {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		return Node{}, false
	}
	sortNodes(candidates)
	if bucket <= 0 || len(candidates) == 1 {
		return candidates[0], true
	}
	return candidates[bucketIndex(candidates, bucket)], true
}

// sortNodes 稳定排序（ID 升序）：保证同一快照下分桶结果可复现。
func sortNodes(nodes []Node) {
	for i := 1; i < len(nodes); i++ {
		for j := i; j > 0 && nodes[j].ID < nodes[j-1].ID; j-- {
			nodes[j], nodes[j-1] = nodes[j-1], nodes[j]
		}
	}
}

func bucketIndex(nodes []Node, bucket int) int {
	if bucket < 0 {
		bucket = -bucket
	}
	return bucket % len(nodes)
}

// BucketOf 把任意业务键映射到 0–255 分桶（§3.1 的 hash_bucket 语义，接入方自定义）。
func BucketOf(key string) int {
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % 256)
}

// New 按 config.Cluster 的 DSN 构造视图客户端（§15.1#3）：
// grpc:// 走 gRPC，http(s):// 走 HTTP(S)/JSON，local://（单节点部署，空 DSN 同）走本地静态视图。
// 连接与调用参数（timeout / connect_timeout / interval）由 ParseClusterDSN 统一解析。
func New(cfg config.Cluster) (View, error) {
	opts, err := config.ParseClusterDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	switch opts.Scheme {
	case config.ClusterSchemeHTTP, config.ClusterSchemeHTTPS:
		return NewHTTPView(opts)
	case config.ClusterSchemeLocal:
		return NewStaticView(opts), nil
	case config.ClusterSchemeGRPC:
		return NewGRPCView(opts)
	default:
		// 未知模式按 static 处理会掩盖配置错误，显式报错更安全。
		return nil, errUnknownScheme(string(opts.Scheme))
	}
}

// Cache 是带快照的视图包装：后台周期刷新（PollInterval > 0），读路径无网络 I/O，
// 供转发器与分发器在热路径上同步解析节点地址。刷新失败保留上一份快照并记录错误，
// 由调用方决定是否降级（集群视图是只读提示，PG 仍是唯一权威）。
type Cache struct {
	view     View
	interval time.Duration

	mu   sync.RWMutex
	info Info
	err  error

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewCache 包装一个 View 并立即做一次同步拉取；失败不返回错误（保留空快照，等待后续刷新）。
func NewCache(ctx context.Context, view View, interval time.Duration) *Cache {
	c := &Cache{view: view, interval: interval, stop: make(chan struct{}), done: make(chan struct{})}
	_ = c.refresh(ctx)
	if interval > 0 {
		go c.loop()
	} else {
		close(c.done)
	}
	return c
}

// Refresh 主动拉取一次。
func (c *Cache) Refresh(ctx context.Context) error { return c.refresh(ctx) }

func (c *Cache) refresh(ctx context.Context) error {
	info, err := c.view.Get(ctx)
	c.mu.Lock()
	if err == nil {
		c.info = info
	}
	c.err = err
	c.mu.Unlock()
	return err
}

func (c *Cache) loop() {
	defer close(c.done)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.interval)
			_ = c.refresh(ctx)
			cancel()
		}
	}
}

// Snapshot 返回最近一次成功拉取的快照（可能为空）。
func (c *Cache) Snapshot() Info {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.info
}

// Err 返回最近一次刷新错误。
func (c *Cache) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

// Node 按 ID 解析节点（热路径，无网络 I/O）。
func (c *Cache) Node(id string) (Node, bool) { return c.Snapshot().Node(id) }

// Scheduler 返回当前调度节点快照。
func (c *Cache) Scheduler() (Node, bool) { return c.Snapshot().Scheduler() }

// Select 基于最近快照选执行节点。
func (c *Cache) Select(nodeID, vpc, label string, bucket int) (Node, bool) {
	return Select(c.Snapshot().Nodes, nodeID, vpc, label, bucket)
}

// Close 停止后台刷新；幂等。
func (c *Cache) Close() error {
	c.stopOnce.Do(func() { close(c.stop) })
	if c.interval > 0 {
		<-c.done
	}
	return nil
}

// Resolver 是热路径地址解析的最小接口，便于测试替身注入。
type Resolver interface {
	Node(id string) (Node, bool)
	Nodes() []Node
}

// Nodes 返回快照节点集合。
func (c *Cache) Nodes() []Node { return c.Snapshot().Nodes }

// atomicInfo 供需要无锁快照的实现复用。
type atomicInfo struct{ v atomic.Value }

func (a *atomicInfo) store(i Info) { a.v.Store(i) }
func (a *atomicInfo) load() Info {
	if i, ok := a.v.Load().(Info); ok {
		return i
	}
	return Info{}
}

// ---- proto ↔ 领域转换 ----

// fromProto 把外部集群视图的 NodeInfo 转为领域节点（§15.1#3）。
func fromProto(n *clusterv1.NodeInfo) Node {
	if n == nil {
		return Node{}
	}
	return Node{
		ID:          n.GetNodeId(),
		Address:     n.GetAddress(),
		VPC:         n.GetVpc(),
		Labels:      append([]string(nil), n.GetLabels()...),
		Roles:       append([]string(nil), n.GetRole()...),
		Permissions: permissionsOf(n.GetRole()),
		Online:      n.GetOnline(),
		IsLeader:    n.GetIsLeader(),
	}
}

func infoFromProto(resp *clusterv1.ClusterInfoResponse) Info {
	if resp == nil {
		return Info{}
	}
	info := Info{NodeID: resp.GetNodeId()}
	for _, n := range resp.GetNodes() {
		node := fromProto(n)
		if node.IsLeader && info.SchedulerNodeID == "" {
			info.SchedulerNodeID = node.ID
		}
		info.Nodes = append(info.Nodes, node)
	}
	return info
}

// ---- 错误辅助 ----

func errUnknownScheme(t string) error {
	return fmt.Errorf("cluster: unknown dsn scheme %q (want grpc|http|local)", t)
}

func errNoEndpoint(transport string) error {
	return fmt.Errorf("cluster: dsn scheme %s requires host (e.g. grpc://host:port)", transport)
}
