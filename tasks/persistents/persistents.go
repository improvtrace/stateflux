// Package persistents 是持久化转发任务的消费侧：调度框架的 Dispatcher 把带
// AttributePersistent 标记的 Signature 包装成 persistent_forward 投递给 leader
// 节点；本包收到后先落 task_persistents（dispatch 前持久化），再把内层
// Signature 解码并重新发布到执行队列。目标在于其他节点只经 Dispatcher 即可
// 发布持久化签名，DB 写入收敛在 leader。
//
// 本包不生产任务（无 Cron/Factory）：生产者按发布约定组装待持久化的
// Signature（Name=资源名、Args=[payload JSON, taskID]、headers 携带 Attribute
// 路由属性）并标记 AttributePersistent 后发布即可。
package persistents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/wire"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

const (
	// Resource 是转发任务的路由资源名（AttributeResource），分节点队列为
	// queue:persistents/<node>。
	Resource = dispatch.PersistentResource

	// TaskName 是转发任务的 handler 注册名（Signature.Name）。
	TaskName = dispatch.PersistentForwardName
)

// Publisher 是转发通道的消费侧小接口：把内层 Signature 重新发布到执行队列。
// *dispatch.Dispatcher 实现本接口；接口独立便于测试注入。
type Publisher interface {
	Dispatch(ctx context.Context, sig *queue.Signature) (*queue.AsyncResult, error)
}

// Forwarder 是 persistent_forward 的 handler：落表 → 认领 → 重发 → 确认。
// 重发后的执行签名与表行对齐（UUID=task_{id}、第二 Arg=id），成功回写与对账
// 均按表行协议收敛。
type Forwarder struct {
	repo states.TaskPersistentRepo
	pub  Publisher
	node string // 本节点 ID，认领时写 claimed_node（诊断用）
}

// New 构造转发处理器。node 是本节点 ID（dm 查询）。
func New(repo states.TaskPersistentRepo, pub Publisher, node string) *Forwarder {
	return &Forwarder{repo: repo, pub: pub, node: node}
}

// ProviderSet 是本任务的 wire 装配单元：Publisher 接口由根装配集的
// *dispatch.Dispatcher 经 wire.Bind 绑定（见 entry.go ProviderSet）。
var ProviderSet = wire.NewSet(New)

// Spec 返回转发任务定义：非 Durable（转发任务自身不落表，避免自指）、无
// Cron/Factory（本包不自己生产）。SignatureName 与 Resource 不同：队列按
// persistents 路由，handler 按 persistent_forward 注册。
func Spec(f *Forwarder) *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:      Resource,
		SignatureName: TaskName,
		Enabled:       true,
		Concurrency:   4,
		Handler:       f.Handle,
		Timeout:       30 * time.Second,
		Retries:       dispatch.PersistentForwardRetries,
		Durable:       false,
	}
}

// Handle 处理一条转发任务：解码内层 Signature → 落表（幂等）→ 认领 →
// 重新发布 → 确认。重复投递（队列重试、重复转发）由幂等键与阶段判断去重：
// 行已不在 pending 时不再重发。
//
// 失败语义：落表/认领失败直接返回错误，由队列按转发任务的重试次数重投；
// 重发失败把行退回 pending 并返回错误（下一轮尝试或对账恢复通道重派）；
// 重发成功但确认失败只记日志——行停留在 dispatching，由对账按执行终态收敛，
// 不重发（避免重复执行）。
func (f *Forwarder) Handle(payload string, _ int64) error {
	ctx := context.Background()
	sig, err := decode(payload)
	if err != nil {
		return err
	}
	row, err := f.persist(ctx, sig)
	if err != nil {
		return err
	}
	if row.Phase != states.TaskPhasePending {
		slog.Debug("persistents: signature already handled",
			"task_id", row.Id, "resource", row.Resource, "phase", row.Phase)
		return nil
	}
	claimed, err := f.repo.Claim(ctx, row.Id, f.node)
	if err != nil {
		return fmt.Errorf("persistents: claim task %d: %w", row.Id, err)
	}
	if !claimed {
		// 并发转发或恢复通道抢先认领：发布权归认领方
		slog.Debug("persistents: task already claimed", "task_id", row.Id, "resource", row.Resource)
		return nil
	}
	// 执行侧协议：UUID/第二 Arg 必须与表行一致（对账按 task_{id} 查执行状态，
	// 成功回写按第二 Arg 落终态）
	sig.UUID = states.SignatureUUID(row.Id)
	sig.Args[1] = queue.Arg{Type: "int64", Value: row.Id}
	result, err := f.pub.Dispatch(ctx, sig)
	if err != nil {
		if rerr := f.repo.Requeue(ctx, row.Id); rerr != nil {
			slog.Error("persistents: failed to requeue task after dispatch failure",
				"task_id", row.Id, "error", rerr)
		}
		return fmt.Errorf("persistents: republish task %d: %w", row.Id, err)
	}
	if err := f.repo.MarkDispatched(ctx, []int64{row.Id}); err != nil {
		// 发布已成功，确认失败不重发（会重复执行）：行停留在 dispatching，
		// 由对账按执行终态收敛
		slog.Error("persistents: failed to confirm dispatched task",
			"task_id", row.Id, "error", err)
	}
	uuid := ""
	if result != nil && result.Signature != nil {
		uuid = result.Signature.UUID
	}
	slog.Debug("persistents: signature persisted and republished",
		"task_id", row.Id, "resource", row.Resource, "uuid", uuid)
	return nil
}

// persist 把内层 Signature 映射为表行并落表。Create 对幂等键冲突幂等
// （返回已有行），重复转发不会重复落表。
func (f *Forwarder) persist(ctx context.Context, sig *queue.Signature) (*states.TaskPersistent, error) {
	row, err := rowFromSignature(sig)
	if err != nil {
		return nil, err
	}
	created, err := f.repo.Create(ctx, row)
	if err != nil {
		return nil, fmt.Errorf("persistents: persist task %d: %w", row.Id, err)
	}
	return created, nil
}

// decode 解码转发参数：唯一 string Arg 承载 {"signature": "..."}，signature
// 是原 Signature 的 JSON 编码（UseNumber 保持整数精确）。headers 一并还原为
// dispatcher 认识的类型，保证内层签名可再次路由。
func decode(payload string) (*queue.Signature, error) {
	var p dispatch.PersistentForwardPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return nil, fmt.Errorf("persistents: decode forward payload: %w", err)
	}
	if p.Signature == "" {
		return nil, errors.New("persistents: forward payload carries no signature")
	}
	sig := new(queue.Signature)
	decoder := json.NewDecoder(strings.NewReader(p.Signature))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		return nil, fmt.Errorf("persistents: decode signature: %w", err)
	}
	// 正常路径由 Dispatcher 包装时已剥掉标记；生产方若绕过 Dispatcher 直发，
	// 这里也剥掉，避免重发时再次触发转发（多绕一跳）
	delete(sig.Headers, dispatch.AttributePersistent)
	if err := normalizeHeaders(sig.Headers); err != nil {
		return nil, err
	}
	return sig, nil
}

// rowFromSignature 把内层 Signature 映射为 task_persistents 行：Id 取自发布
// 约定（第二 Arg 的 taskID，缺省回退 UUID 的 task_{id}），Resource 取
// AttributeResource（缺省 Name），Payload 取第一 Arg（JSON 字符串）；Vpc/
// Node/HashBucket/Label 从 Attribute headers 还原，保证对账重派时路由约束不丢。
func rowFromSignature(sig *queue.Signature) (*states.TaskPersistent, error) {
	resource := headerString(sig.Headers, dispatch.AttributeResource)
	if resource == "" {
		resource = sig.Name
	}
	if resource == "" {
		return nil, errors.New("persistents: signature carries neither AttributeResource nor Name")
	}
	if len(sig.Args) != 2 {
		return nil, fmt.Errorf("persistents: signature of %s must carry [payload, taskID] args, got %d",
			resource, len(sig.Args))
	}
	payload, ok := sig.Args[0].Value.(string)
	if !ok {
		return nil, fmt.Errorf("persistents: signature of %s: first arg must be a string payload", resource)
	}
	if payload != "" && !json.Valid([]byte(payload)) {
		return nil, fmt.Errorf("persistents: signature of %s: payload is not valid JSON", resource)
	}
	id, err := signatureTaskID(sig)
	if err != nil {
		return nil, err
	}
	// NewRow 填充 Row 的缺省预算/重试预算；转发侧不持有任务 spec（装配环），
	// 只能取框架缺省值，执行内重试仍按内层签名的 RetryCount。
	row := (&tasks.TaskSpec{Resource: resource}).NewRow(json.RawMessage(payload))
	row.Id = id
	// 幂等键取 Signature UUID：重复转发同一签名不重复落表；UUID 缺失时按
	// 表行协议 task_{id} 生成，重复投递仍幂等。
	row.IdempotencyKey = sig.UUID
	if row.IdempotencyKey == "" {
		row.IdempotencyKey = states.SignatureUUID(row.Id)
	}
	row.Vpc = headerString(sig.Headers, dispatch.AttributeVpcs)
	row.Node = headerString(sig.Headers, dispatch.AttributeTargetNode)
	if bucket := headerString(sig.Headers, dispatch.AttributeHashBucket); bucket != "" {
		n, err := strconv.ParseInt(bucket, 10, 16)
		if err != nil || n < 0 || n > 255 {
			return nil, fmt.Errorf("persistents: signature of %s: header %s must be 0-255, got %q",
				resource, dispatch.AttributeHashBucket, bucket)
		}
		row.HashBucket = int16(n)
	}
	labels, err := headerLabels(sig.Headers)
	if err != nil {
		return nil, err
	}
	row.Label = labelsString(labels)
	return row, nil
}

// signatureTaskID 解析表行 ID：优先第二 Arg（发布约定的 taskID），缺省回退
// UUID 协议 task_{id}。ID 由生产方生成（与发布通道一致，转发侧不引入第二套
// 雪花节点），两者都没有即拒绝。
func signatureTaskID(sig *queue.Signature) (int64, error) {
	if len(sig.Args) >= 2 {
		switch v := sig.Args[1].Value.(type) {
		case int64:
			if v > 0 {
				return v, nil
			}
		case json.Number:
			if id, err := v.Int64(); err == nil && id > 0 {
				return id, nil
			}
		}
	}
	if id, ok := taskIDFromUUID(sig.UUID); ok {
		return id, nil
	}
	return 0, fmt.Errorf("persistents: signature %q carries no task id (args[1] or task_{id} UUID); the producer must allocate it",
		sig.UUID)
}

// taskIDFromUUID 解析发布协议 UUID task_{id}。
func taskIDFromUUID(uuid string) (int64, bool) {
	id, ok := strings.CutPrefix(uuid, "task_")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// normalizeHeaders 把 JSON 解码回来的 headers 还原为 dispatcher 认识的类型：
// AttributeLabels 经 JSON 往返变成 map[string]any（值为 []any），直接重发会
// 因类型不符被 dispatch 拒绝，这里还原为 map[string][]string。
func normalizeHeaders(h queue.Headers) error {
	v, ok := h[dispatch.AttributeLabels]
	if !ok || v == nil {
		return nil
	}
	switch labels := v.(type) {
	case map[string][]string:
		return nil
	case map[string]any:
		out := make(map[string][]string, len(labels))
		for key, raw := range labels {
			values, ok := raw.([]any)
			if !ok {
				return fmt.Errorf("persistents: header %s[%s] must be a list of strings",
					dispatch.AttributeLabels, key)
			}
			converted := make([]string, len(values))
			for i, item := range values {
				value, ok := item.(string)
				if !ok {
					return fmt.Errorf("persistents: header %s[%s] must be a list of strings",
						dispatch.AttributeLabels, key)
				}
				converted[i] = value
			}
			out[key] = converted
		}
		h[dispatch.AttributeLabels] = out
		return nil
	default:
		return fmt.Errorf("persistents: header %s must be map[string][]string", dispatch.AttributeLabels)
	}
}

// headerLabels 取标签约束（normalizeHeaders 之后值应为 map[string][]string）。
// 类型不符报错而不是忽略：静默丢弃约束会把任务投到不满足约束的节点。
func headerLabels(h queue.Headers) (map[string][]string, error) {
	v, ok := h[dispatch.AttributeLabels]
	if !ok || v == nil {
		return nil, nil
	}
	labels, ok := v.(map[string][]string)
	if !ok {
		return nil, fmt.Errorf("persistents: header %s must be map[string][]string", dispatch.AttributeLabels)
	}
	return labels, nil
}

// headerString 取字符串 header。
func headerString(h queue.Headers, key string) string {
	v, _ := h[key].(string)
	return v
}

// labelsString 把标签约束序列化为表列格式 key=value;…（同键多值重复出现），
// 与 dispatch.ParseLabels 互逆。键与值排序保证同一约束的落表结果稳定。
func labelsString(labels map[string][]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		values := append([]string(nil), labels[key]...)
		sort.Strings(values)
		for _, value := range values {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, ";")
}
