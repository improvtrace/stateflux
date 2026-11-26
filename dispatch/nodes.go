package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Signature headers 键（Attribute 前缀，attachRoutingKey 的路由依据）：
// 组装 Signature 时写入，发布前与集群节点表匹配出目标节点。
const (
	AttributeResource = "AttributeResource" // 任务资源名（= task_persistents.resource）
	AttributeVpcs     = "AttributeVpcs"     // 可执行的 vpc 环境，多值以英文分号间隔
	AttributeRoles    = "AttributeRoles"    // 限定执行的节点角色，多值以英文分号间隔
	AttributeLabels   = "AttributeLabels"   // 要求的节点标签，map[string][]string（ParseLabels 的产物）
	// 选点偏好（pickNode）：两者都不影响候选过滤，只在候选内优先选定。
	AttributeClaimedNode = "AttributeClaimedNode" // 上次选择的节点，优先级最高
	AttributeTargetNode  = "AttributeTargetNode"  // 用户指定的节点，次于 ClaimedNode
	// 散列键：非空时取代 Signature.UUID 作哈希输入（同桶任务落到同一节点）。
	AttributeHashBucket = "AttributeHashBucket" // 哈希桶（= task_persistents.hash_bucket）
)

// QueueName 生成分节点任务队列名：queue:<resource>/<node>。发布侧
// （attachRoutingKey）与消费侧（worker 启动）共用此约定。
func QueueName(resource, node string) string {
	return fmt.Sprintf("queue:%s/%s", resource, node)
}

// nodeTableTTL 是节点表的固定缓存失效时间：节点上下线经 dm 侧变化后，
// 发布侧至多一个 TTL 内感知。
const nodeTableTTL = 10 * time.Second

// Node 是节点表中的一条节点记录，对齐 dm 服务 ListClusterNodesStatus 的节点
// 字段。Vpcs 为节点可执行的 vpc 环境列表、Labels 为节点标签（键 → 多值），
// 供 AttributeVpcs / AttributeLabels 匹配；两者 dm 节点信息暂未携带，待节点
// 属性扩展后填充。
type Node struct {
	Id     string              // 节点 ID
	Name   string              // 节点名称
	Ip     string              // 节点 IP
	Role   string              // 节点角色（LEADER/FOLLOWER）
	Online bool                // 是否在线
	Vpcs   []string            // 可执行的 vpc 环境，空 = 未声明（不匹配带 vpc 约束的任务）
	Labels map[string][]string // 节点标签，未声明 = 不匹配带标签约束的任务
}

// ParseLabels 解析标签串为 键 → 多值：英文分号间隔的 key=value 对，同键多次
// 出现累加为多值，空白片段忽略。例如：
//
//	"disk=ssd;zone=a;zone=b" → {"disk": ["ssd"], "zone": ["a", "b"]}
//
// 片段缺少 = 或键、值为空即报错：约束解析失败不能静默放弃，否则任务会被
// 投到不满足约束的节点。
func ParseLabels(s string) (map[string][]string, error) {
	labels := map[string][]string{}
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("dispatch: label %q must be key=value", part)
		}
		labels[key] = append(labels[key], value)
	}
	return labels, nil
}

// nodeTable 是 Dispatcher 内置的集群节点表：经 dm 服务查询（unix socket 懒
// 连接 + 服务密钥拦截器，见 dm.go），按 TTL 固定时间失效，失效后首次访问时
// 重取。表为只读共享，取到空列表同样缓存一个 TTL。
type nodeTable struct {
	mu      sync.Mutex
	dm      *dmCluster
	nodes   map[string]Node // 按 ID 索引
	fetched time.Time       // 上次刷新时间，IsZero 表示尚未取过
}

func newNodeTable(address, serviceKey string) *nodeTable {
	return &nodeTable{dm: newDMCluster(address, serviceKey)}
}

// get 返回节点表（只读），缓存失效时先刷新；刷新失败不清空旧表，返回错误
// 交调用方中止本次发布（持久化行由调用方回收重排，非 Durable 由下个周期
// 覆盖），下轮发布重试再刷新。
func (t *nodeTable) get(ctx context.Context) (map[string]Node, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.nodes != nil && time.Since(t.fetched) < nodeTableTTL {
		slog.Debug("dispatch: node table cache hit",
			"nodes", len(t.nodes), "age", time.Since(t.fetched).Truncate(time.Millisecond))
		return t.nodes, nil
	}
	start := time.Now()
	resp, err := t.dm.listClusterNodesStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch: query cluster nodes: %w", err)
	}
	nodes := make(map[string]Node, len(resp.Nodes))
	online := 0
	for _, n := range resp.Nodes {
		if n.GetOnline() {
			online++
		}
		nodes[n.GetId()] = Node{
			Id:     n.GetId(),
			Name:   n.GetName(),
			Ip:     n.GetIp(),
			Role:   n.GetRole(),
			Online: n.GetOnline(),
		}
	}
	t.nodes = nodes
	t.fetched = time.Now()
	slog.Debug("dispatch: node table refreshed",
		"nodes", len(nodes), "online", online, "duration_ms", time.Since(start).Milliseconds())
	return nodes, nil
}
