// Package dispatch 是调度框架唯一的发布出口：RoutingKey 为空的 Signature
// 在投递前依据 Attribute headers 与集群节点表匹配出目标节点，生成分节点
// 路由（queue:<resource>/<node>），任务由目标节点的 worker 消费；已指定
// RoutingKey 的直接投递。带 AttributePersistent 标记的 Signature 先包装成
// persistent_forward 转发给 leader，由 leader 落表后再重发（见 persistent.go）。
// 本包不感知 task_persistents 表行与状态流转（组装 Signature、发布确认与
// 失败回收由 publisher/调度器与 tasks/persistents 承担）。
package dispatch

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/improvtrace/stateflux/queue"
)

// Dispatcher 是唯一的队列投递出口，两类调用方：Publisher（表行组装的
// Signature）与 CronScheduler（非 Durable 定时直发）。内置集群节点表
// （经 dm 服务查询，固定时间缓存失效），供发布前的目标节点匹配。
type Dispatcher struct {
	server *queue.Server
	nodes  *nodeTable
}

// New 构造发布器。dmAddress 是 dm 服务的 unix socket 监听地址，节点表经其
// 查询；serviceKey 是 dm 内部接口的服务密钥。
func New(server *queue.Server, dmAddress, serviceKey string) *Dispatcher {
	slog.Debug("dispatch: dispatcher initialized", "dm_address", dmAddress)
	return &Dispatcher{server: server, nodes: newNodeTable(dmAddress, serviceKey)}
}

// Dispatch 发布一条 Signature，返回本次投递的 AsyncResult（可经 GetState 查询
// 状态；持久化转发场景返回的是转发任务 s2 的结果）。RoutingKey 为空时先经
// attachRoutingKey 依据 Attribute headers 匹配目标节点生成路由，匹配不到
// （无在线或满足约束的节点）即拒绝发布。
//
// 带 AttributePersistent 标记的 Signature 不直接投递：去掉标记后包装成
// persistent_forward 转发任务（仅发布给 leader），由 leader 侧先落
// task_persistents 再重新发布（协议见 persistent.go）。
func (d *Dispatcher) Dispatch(ctx context.Context, sig *queue.Signature) (*queue.AsyncResult, error) {
	start := time.Now()
	resource := headerString(sig.Headers, AttributeResource)
	if resource == "" {
		resource = sig.Name
	}
	if _, ok := sig.Headers[AttributePersistent]; ok {
		slog.Debug("dispatch: forwarding persistent signature",
			"resource", resource, "uuid", sig.UUID, "name", sig.Name)
		forward, err := forwardSignature(sig)
		if err != nil {
			return nil, err
		}
		// 转发也是普通发布：递归走同一路由/投递路径（s2 已无标记，不会二次转发）
		return d.Dispatch(ctx, forward)
	}
	slog.Debug("dispatch: publishing task",
		"resource", resource, "uuid", sig.UUID, "routing_key", sig.RoutingKey)
	if sig.RoutingKey == "" {
		if err := d.attachRoutingKey(ctx, sig); err != nil {
			return nil, err
		}
	}
	result, err := d.server.SendTaskWithContext(ctx, sig)
	if err != nil {
		return nil, fmt.Errorf("dispatch: publish %s: %w", sig.Name, err)
	}
	slog.Debug("dispatch: task sent",
		"resource", resource, "uuid", sig.UUID, "routing_key", sig.RoutingKey,
		"duration_ms", time.Since(start).Milliseconds())
	return result, nil
}

// attachRoutingKey 为未指定路由的 Signature 计算目标队列：取 headers 中的
// AttributeResource/AttributeVpcs/AttributeRoles/AttributeLabels 过滤节点表
// 得到候选（在线且满足约束），再由 pickNode 选定目标节点，写入
// queue:<resource>/<node>。resource 缺省取 Signature.Name（handler 同名
// 注册）；无候选即拒绝发布。
func (d *Dispatcher) attachRoutingKey(ctx context.Context, sig *queue.Signature) error {
	nodes, err := d.nodes.get(ctx)
	if err != nil {
		return err
	}
	resource := headerString(sig.Headers, AttributeResource)
	if resource == "" {
		resource = sig.Name
	}
	labels, err := headerLabels(sig.Headers)
	if err != nil {
		return err
	}
	vpcs := headerList(sig.Headers, AttributeVpcs)
	roles := headerList(sig.Headers, AttributeRoles)
	candidates := matchNodes(nodes, vpcs, roles, labels)
	slog.Debug("dispatch: routing candidates matched",
		"resource", resource, "candidates", len(candidates), "nodes", len(nodes))
	if len(candidates) == 0 {
		slog.Warn("dispatch: no matching execution node",
			"resource", resource, "vpcs", vpcs, "roles", roles, "labels", labels)
		return fmt.Errorf("dispatch: no matching execution node for %s", resource)
	}
	node := pickNode(sig, candidates)
	sig.RoutingKey = QueueName(resource, node)
	slog.Debug("dispatch: routing key attached",
		"resource", resource, "node", node, "routing_key", sig.RoutingKey)
	return nil
}

// matchNodes 按 AttributeVpcs/AttributeRoles/AttributeLabels 过滤在线节点：
// vpcs 非空时节点 Vpcs 须与其有交集（节点未声明 Vpcs 不匹配）；roles 非空时
// 节点 Role 须在集合内；labels 非空时须满足 hasLabels。返回候选按 ID 排序，
// 保证选定结果确定。
func matchNodes(nodes map[string]Node, vpcs, roles []string, labels map[string][]string) []Node {
	var candidates []Node
	for _, n := range nodes {
		if !n.Online {
			continue
		}
		if len(vpcs) > 0 && !intersects(n.Vpcs, vpcs) {
			continue
		}
		if len(roles) > 0 && !contains(roles, n.Role) {
			continue
		}
		if !hasLabels(n, labels) {
			continue
		}
		candidates = append(candidates, n)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Id < candidates[j].Id })
	return candidates
}

// hasLabels 判断节点是否满足标签约束：每个键节点都须声明，且在该键上的取值
// 与约束有交集（vpc 交集规则按键的推广）；labels 为空即无约束。
func hasLabels(n Node, labels map[string][]string) bool {
	for key, want := range labels {
		if !intersects(n.Labels[key], want) {
			return false
		}
	}
	return true
}

// pickNode 从候选节点中选定目标节点，优先级从高到低：
//   - AttributeClaimedNode：上次选择的节点，仍在候选内即沿用；
//   - AttributeTargetNode：用户指定的节点，仍在候选内即采用；
//   - 散列取模：哈希键取 AttributeHashBucket（同桶的任务落到同一节点），
//     未设置时取 Signature UUID——同一任务重派路由稳定，批次内各任务在
//     候选节点间散列。
//
// 偏好的节点不在候选内（离线或不满足约束）时顺延到下一级，不拒绝发布。
func pickNode(sig *queue.Signature, candidates []Node) string {
	for _, key := range [...]string{AttributeClaimedNode, AttributeTargetNode} {
		if id := headerString(sig.Headers, key); id != "" &&
			slices.ContainsFunc(candidates, func(n Node) bool { return n.Id == id }) {
			return id
		}
	}
	key := sig.UUID
	if bucket := headerString(sig.Headers, AttributeHashBucket); bucket != "" {
		key = bucket
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return candidates[h.Sum32()%uint32(len(candidates))].Id
}

// headerString 取字符串 header。
func headerString(h queue.Headers, key string) string {
	v, _ := h[key].(string)
	return v
}

// headerList 取分号间隔的多值 header，剔除空片段。
func headerList(h queue.Headers, key string) []string {
	v := headerString(h, key)
	if v == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(v, ";") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// headerLabels 取标签约束 header：值为 ParseLabels 的产物（map[string][]string），
// 未设置即无约束。类型不符报错而不是忽略：静默丢弃约束会把任务投到不满足
// 约束的节点。
func headerLabels(h queue.Headers) (map[string][]string, error) {
	v, ok := h[AttributeLabels]
	if !ok || v == nil {
		return nil, nil
	}
	labels, ok := v.(map[string][]string)
	if !ok {
		return nil, fmt.Errorf("dispatch: header %s must be map[string][]string (see dispatch.ParseLabels)", AttributeLabels)
	}
	return labels, nil
}

// intersects 判断两集合是否有交集。
func intersects(a, b []string) bool {
	set := make(map[string]bool, len(b))
	for _, v := range b {
		set[v] = true
	}
	for _, v := range a {
		if set[v] {
			return true
		}
	}
	return false
}

// contains 判断集合是否包含指定值。
func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
