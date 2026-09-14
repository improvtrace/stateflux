// Package cluster 提供集群视图（ClusterView）外部接口、RPC 客户端与 static 单机
// 实现（§2.1、§8、§15.1#3）。集群信息只读不治：选举由外部系统负责，stateflux 仅消费。
//
// 连接信息来自 config.Cluster（§15.1#3），支持 grpc / http / static 三种传输。
// 本包只做「拉取 + 快照 + 选择」，不做任何调度决策之外的裁决：路由决策仍在调度侧
// （§1.2.7），本包的选择辅助函数只是把 vpc/label/node/bucket 过滤与哈希分桶集中一处。
package cluster

import (
	"context"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/improvtrace/stateflux/internal/config"
)

// Role 是节点角色（§2.1）：worker(executor) 与 biz 常驻，控制面随选举启停。
type Role string

const (
	// RoleBiz 装载 task/v1 服务端业务（§15.1#2）。
	RoleBiz Role = "biz"
	// RoleExecutor 执行节点（worker 运行时）。
	RoleExecutor Role = "executor"
	// RoleScheduler 调度运行时（晋升/认领/分发）。
	RoleScheduler Role = "scheduler"
	// RoleCollector 结果归集运行时。
	RoleCollector Role = "collector"
	// RoleReconcile 对账运行时（R1–R4）。
	RoleReconcile Role = "reconcile"
)

// Node 是集群视图中的一个节点（§15.1#3）：node_id + vpc + label 是拉取的核心三要素，
// address/roles/capabilities 用于通信与路由。
type Node struct {
	// ID 节点唯一标识（集群内稳定）。
	ID string
	// Address 对外可达的 gRPC 地址（host:port）。
	Address string
	// VPC 目标网络域（调度目标节点属性）。
	VPC string
	// Label 节点匹配标签（自由文本，框架不解释语义）。
	Label string
	// Roles 节点角色集合。
	Roles []string
	// Capabilities 能力标签集合（异步队列路由与能力匹配）。
	Capabilities []string
}

// HasRole 判断节点是否承担某角色。
func (n Node) HasRole(r Role) bool {
	for _, role := range n.Roles {
		if strings.EqualFold(role, string(r)) {
			return true
		}
	}
	return false
}

// HasCapability 判断节点是否声明某能力。
func (n Node) HasCapability(name string) bool {
	for _, c := range n.Capabilities {
		if c == name {
			return true
		}
	}
	return false
}

// Info 是一次集群视图快照。
type Info struct {
	// Nodes 当前全部节点。
	Nodes []Node
	// SchedulerNodeID 当前调度节点 ID；空表示集群暂无调度节点（§2.1）。
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
		if label != "" && n.Label != label {
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

// New 按 config.Cluster.Transport 构造视图客户端（§15.1#3）。
func New(cfg config.Cluster, local config.Node) (View, error) {
	switch cfg.Transport {
	case config.ClusterHTTP:
		return NewHTTPView(cfg)
	case config.ClusterStatic, "":
		return NewStaticView(cfg, local), nil
	case config.ClusterGRPC:
		return NewGRPCView(cfg)
	default:
		// 未知传输按 static 处理会掩盖配置错误，显式报错更安全。
		return nil, errUnknownTransport(string(cfg.Transport))
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
