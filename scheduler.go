package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

// defaultListenerBuffer 是事件缓冲大小。缓冲满时事件被丢弃（见 Emit），
// 大小只需覆盖一个触发周期内的事件量。
const defaultListenerBuffer = 256

// Event 是调度事件的封闭集合，目前仅有 CronEvent（cron tick）。
type Event interface{ event() }

// CronEvent 表示某定时任务到点。Resource 即 tasks.TaskSpec.Resource。
type CronEvent struct {
	Resource string
}

func (CronEvent) event() {}

// eventName 返回事件的稳定名称，用于日志与观测。
func eventName(ev Event) string {
	switch e := ev.(type) {
	case CronEvent:
		return "cron:" + e.Resource
	default:
		return fmt.Sprintf("%T", ev)
	}
}

// Handler 是事件的消费者，由各调度器实现。
type Handler interface {
	Name() string
	HandleEvent(ctx context.Context, ev Event)
}

// Listener 是单事件循环：事件串行分发给全部注册 Handler。慢 handler 会
// 阻塞整个循环，调度逻辑必须非阻塞（重活走带超时的调用或交给 dispatcher）。
// ch 不关闭：停止靠 ctx 取消，未消费的事件随对象回收。
type Listener struct {
	handlers []Handler
	ch       chan Event
}

// NewListener 构造事件总线，buffer <=0 时取缺省值。
func NewListener(buffer int) *Listener {
	if buffer <= 0 {
		buffer = defaultListenerBuffer
	}
	return &Listener{ch: make(chan Event, buffer)}
}

// Register 注册事件消费者，须在 Run 之前完成。
func (l *Listener) Register(h Handler) {
	l.handlers = append(l.handlers, h)
	slog.Debug("schedule: listener handler registered", "handler", h.Name())
}

// Emit 非阻塞投递：缓冲满即丢弃并返回 false。定时事件丢了由下个周期补偿。
func (l *Listener) Emit(ev Event) bool {
	select {
	case l.ch <- ev:
		return true
	default:
		slog.Warn("schedule: event dropped, listener buffer is full",
			"event", eventName(ev), "buffer", cap(l.ch))
		return false
	}
}

// Run 消费并分发事件直到 ctx 取消。单个 handler panic 被捕获记日志，
// 不影响循环与其余 handler。
func (l *Listener) Run(ctx context.Context) error {
	slog.Debug("schedule: listener started", "buffer", cap(l.ch))
	for {
		select {
		case <-ctx.Done():
			slog.Debug("schedule: listener stopped")
			return ctx.Err()
		case ev := <-l.ch:
			slog.Debug("schedule: listener received event", "event", eventName(ev), "handlers", len(l.handlers))
			for _, h := range l.handlers {
				l.dispatch(ctx, h, ev)
			}
		}
	}
}

// dispatch 带 panic 恢复地分发单个事件。
func (l *Listener) dispatch(ctx context.Context, h Handler, ev Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("schedule: recovered panic while handling event",
				"handler", h.Name(), "event", eventName(ev), "panic", r)
		}
	}()
	h.HandleEvent(ctx, ev)
}

// cronLockTTL 是定时任务 tick 防重锁的 TTL 上限。五段 cron 最小粒度为
// 1 分钟，30 秒锁不会跨 tick 存活；@every 描述符按周期一半收缩
// （cronLockTTLFor），使亚分钟周期不被锁节流。
const cronLockTTL = 30 * time.Second

// cronLockTTLFor 按定时表达式计算防重锁 TTL：五段 cron 取上限 30s；
// @every 描述符按周期一半收缩，解析失败回退上限。
func cronLockTTLFor(expr string) time.Duration {
	if dur, err := time.ParseDuration(strings.TrimPrefix(expr, "@every ")); err == nil && dur > 0 {
		if half := dur / 2; half < cronLockTTL {
			return half
		}
	}
	return cronLockTTL
}

// CronScheduler 定时触发（cron 表达式，含 robfig/cron 的 @every 等描述符）：
// 内部 cron 到点向 listener 发 CronEvent；HandleEvent 按 Durable 分流——
// Durable 经 factory 生成批次，由 publisher 落表后立即发布（走正常调度路径），
// 非 Durable 自组 Signature 经 dispatcher 直发。多节点部署时同一任务每个
// 节点都会 tick：Durable 靠幂等键去重（正确但浪费），非 Durable 直发会重复
// 执行——tick 前抢短 TTL Redis 锁保证单节点触发；被锁挡掉的 tick 按「下轮
// 周期自然覆盖」语义丢弃。
type CronScheduler struct {
	cron     *cron.Cron
	listener *Listener
	reg      *tasks.Registry
	pub      *Publisher
	disp     *dispatch.Dispatcher
	rdb      redis.UniversalClient
	node     string
}

// NewCronScheduler 为 reg.Timed() 中每个任务建 cron 条目。表达式非法即报错。
func NewCronScheduler(reg *tasks.Registry, pub *Publisher, disp *dispatch.Dispatcher, listener *Listener, rdb redis.UniversalClient, node string) (*CronScheduler, error) {
	s := &CronScheduler{
		cron:     cron.New(),
		listener: listener,
		reg:      reg,
		pub:      pub,
		disp:     disp,
		rdb:      rdb,
		node:     node,
	}
	for _, ts := range reg.Timed() {
		ts := ts
		if _, err := s.cron.AddFunc(ts.Cron, func() {
			s.fire(ts)
		}); err != nil {
			return nil, fmt.Errorf("schedule: register cron task %s: %w", ts.Resource, err)
		}
		slog.Info("schedule: cron task registered",
			"resource", ts.Resource, "cron", ts.Cron,
			"durable", ts.Durable, "retries", ts.Retries)
	}
	return s, nil
}

// fire 抢锁后发 CronEvent。抢锁失败（他节点已触发）记 debug 后丢弃。
func (s *CronScheduler) fire(ts *tasks.TaskSpec) {
	slog.Debug("schedule: cron tick", "resource", ts.Resource, "node", s.node)
	ok, err := s.rdb.SetNX(context.Background(), "schedule_stable:cron:"+ts.Resource, s.node, cronLockTTLFor(ts.Cron)).Result()
	if err != nil {
		slog.Error("schedule: failed to acquire cron lock", "resource", ts.Resource, "error", err)
		return
	}
	if !ok {
		slog.Debug("schedule: cron tick skipped, lock held by another node", "resource", ts.Resource)
		return
	}
	if s.listener.Emit(CronEvent{Resource: ts.Resource}) {
		slog.Debug("schedule: cron event emitted", "resource", ts.Resource)
	}
}

// Name 实现 Handler。
func (s *CronScheduler) Name() string { return "cron" }

// HandleEvent 处理定时事件：非 Durable 直发；Durable 经 factory 生成批次，
// 由 publisher 落表后发布。
func (s *CronScheduler) HandleEvent(ctx context.Context, ev Event) {
	ce, ok := ev.(CronEvent)
	if !ok {
		slog.Warn("schedule: unexpected event type", "type", fmt.Sprintf("%T", ev))
		return
	}
	start := time.Now()
	ts, ok := s.reg.Get(ce.Resource)
	if !ok {
		slog.Warn("schedule: event for unregistered resource", "resource", ce.Resource)
		return
	}
	if !ts.Durable {
		if _, err := s.disp.Dispatch(ctx, s.statelessSignature(ts)); err != nil {
			slog.Error("schedule: failed to dispatch stateless task", "resource", ts.Resource, "error", err)
			return
		}
		slog.Info("schedule: stateless task dispatched",
			"resource", ts.Resource, "duration_ms", time.Since(start).Milliseconds())
		return
	}
	dts, err := ts.Factory.Generate(ctx, ts.FactoryBatch)
	if err != nil {
		slog.Error("schedule: failed to generate task batch", "resource", ts.Resource, "error", err)
		return
	}
	slog.Debug("schedule: task batch generated",
		"resource", ts.Resource, "count", len(dts), "batch", ts.FactoryBatch)
	s.pub.Dispatch(ctx, dts)
	slog.Info("schedule: cron batch processed",
		"resource", ts.Resource, "count", len(dts), "duration_ms", time.Since(start).Milliseconds())
}

// statelessSignature 组装非 Durable 定时任务的直发 Signature：无表行
// （taskID=0，UUID 带 stateless_ 前缀），丢了由下个周期覆盖，不进对账；
// RoutingKey 留空，由 dispatch 按 AttributeResource 匹配目标节点生成分节点
// 路由；其余字段与 Durable 行的组装规则一致。
func (s *CronScheduler) statelessSignature(ts *tasks.TaskSpec) *queue.Signature {
	return &queue.Signature{
		UUID: fmt.Sprintf("stateless_%s_%d", ts.Resource, time.Now().UnixMilli()),
		Name: ts.HandlerName(),
		Args: []queue.Arg{
			{Type: "string", Value: string(ts.Payload)},
			{Type: "int64", Value: int64(0)},
		},
		Headers:    queue.Headers{dispatch.AttributeResource: ts.Resource},
		RetryCount: ts.Retries,
	}
}

// Run 启动内部 cron，阻塞到 ctx 取消。
func (s *CronScheduler) Run(ctx context.Context) error {
	s.cron.Start()
	<-ctx.Done()
	<-s.cron.Stop().Done()
	return ctx.Err()
}

// defaultDispatchBatch 是发布通道单轮认领的行数上限。
const defaultDispatchBatch = 100

// Publisher 是统一的发布通道（两阶段）：落表 → 认领（pending→dispatching）→
// 组装 Signature 并发布 → 确认（→dispatched）。两类调用方共用：
//   - CronScheduler：定时到点，factory 产物先落表再立即发布；
//   - Reconciler：启动/对账恢复，把 pending 行（崩溃残留与 requeue 行）
//     重新 dispatch 到队列。
//
// 发布失败的行 Requeue 回 pending，由下一轮恢复通道带走；发布中途崩溃的行
// 停留在 dispatching，由对账回收。由此是至少一次投递，消费端按 IdempotencyKey
// 幂等。
type Publisher struct {
	repo   states.TaskPersistentRepo
	disp   *dispatch.Dispatcher
	reg    *tasks.Registry
	ledger *states.Ledger
	node   string // 本节点 ID，写入 claimed_node
}

// NewPublisher 构造发布通道。
func NewPublisher(repo states.TaskPersistentRepo, disp *dispatch.Dispatcher, reg *tasks.Registry, ledger *states.Ledger, node string) *Publisher {
	return &Publisher{repo: repo, disp: disp, reg: reg, ledger: ledger, node: node}
}

// Dispatch 发布一批 factory 产物（CronScheduler 定时到点调用）：持久化任务
// 先落表（dispatch 前持久化，Phase 置 pending），再经 DispatchPending 认领
// 发布；无状态任务直接发布。落表幂等键冲突返回已有行，重复 tick 不会重复
// 发布；冲突命中的行若尚未发布，同样由 DispatchPending 带走。
func (p *Publisher) Dispatch(ctx context.Context, batch []tasks.DispatchableTask) {
	start := time.Now()
	seen := make(map[string]bool)
	var resources []string
	var persisted, stateless, failed int
	for _, dt := range batch {
		if dt.Task == nil {
			if _, err := p.disp.Dispatch(ctx, dt.Sig); err != nil {
				failed++
				slog.Error("schedule: failed to dispatch stateless task",
					"resource", dt.Sig.Name, "uuid", dt.Sig.UUID, "error", err)
				continue
			}
			stateless++
			slog.Debug("schedule: stateless task dispatched", "resource", dt.Sig.Name, "uuid", dt.Sig.UUID)
			continue
		}
		row, err := p.repo.Create(ctx, dt.Task)
		if err != nil {
			failed++
			slog.Error("schedule: failed to persist task row",
				"resource", dt.Task.Resource, "task_id", dt.Task.Id, "error", err)
			continue
		}
		persisted++
		slog.Debug("schedule: task row persisted",
			"resource", row.Resource, "task_id", row.Id, "phase", row.Phase)
		if !seen[dt.Task.Resource] {
			seen[dt.Task.Resource] = true
			resources = append(resources, dt.Task.Resource)
		}
	}
	slog.Debug("schedule: publish batch processed",
		"tasks", len(batch), "persisted", persisted, "stateless", stateless,
		"failed", failed, "duration_ms", time.Since(start).Milliseconds())
	p.DispatchPending(ctx, resources)
}

// DispatchPending 认领并发布一批 pending 行（恢复通道）：resources 为空 =
// 全部启用的资源（TaskSpec.Enabled；禁用任务的行不认领，冻结在 pending，
// 重新启用后由本通道带走）。逐行组装 Signature 并发布，成功的行记账后
// MarkDispatched，失败的行 Requeue 回 pending（下一轮恢复通道重试）。
func (p *Publisher) DispatchPending(ctx context.Context, resources []string) {
	start := time.Now()
	if len(resources) == 0 {
		for _, ts := range p.reg.Enabled() {
			resources = append(resources, ts.Resource)
		}
		if len(resources) == 0 {
			slog.Debug("schedule: no enabled tasks, skip dispatching pending rows")
			return
		}
	}
	rows, err := p.repo.ClaimPending(ctx, &states.TaskPersistentClaim{
		Resources: resources,
		Batch:     defaultDispatchBatch,
		Node:      p.node,
	})
	if err != nil {
		slog.Error("schedule: failed to claim pending tasks", "resources", resources, "error", err)
		return
	}
	if len(rows) == 0 {
		slog.Debug("schedule: no pending tasks to dispatch", "resources", resources)
		return
	}
	slog.Info("schedule: pending tasks claimed", "count", len(rows), "resources", resources)

	okIDs := make([]int64, 0, len(rows))
	requeued := 0
	for _, row := range rows {
		sig, err := p.signature(row)
		if err != nil {
			slog.Error("schedule: failed to build task signature",
				"task_id", row.Id, "resource", row.Resource, "error", err)
			p.requeue(ctx, row)
			requeued++
			continue
		}
		if _, err := p.disp.Dispatch(ctx, sig); err != nil {
			slog.Error("schedule: failed to dispatch task",
				"task_id", row.Id, "resource", row.Resource, "uuid", sig.UUID, "error", err)
			p.requeue(ctx, row)
			requeued++
			continue
		}
		if err := p.ledger.Record(ctx, sig.UUID, row.Id); err != nil {
			// 加速索引，失败不阻断：对账兜底
			slog.Warn("schedule: failed to record task in ledger",
				"task_id", row.Id, "uuid", sig.UUID, "error", err)
		}
		slog.Debug("schedule: task dispatched",
			"task_id", row.Id, "resource", row.Resource, "uuid", sig.UUID)
		okIDs = append(okIDs, row.Id)
	}
	if len(okIDs) == 0 {
		slog.Warn("schedule: publish batch completed without dispatch",
			"claimed", len(rows), "requeued", requeued,
			"duration_ms", time.Since(start).Milliseconds())
		return
	}
	if err := p.repo.MarkDispatched(ctx, okIDs); err != nil {
		slog.Error("schedule: failed to confirm dispatched tasks", "count", len(okIDs), "error", err)
	} else {
		slog.Debug("schedule: dispatched tasks confirmed", "count", len(okIDs))
	}
	slog.Info("schedule: publish batch completed",
		"claimed", len(rows), "dispatched", len(okIDs), "requeued", requeued,
		"duration_ms", time.Since(start).Milliseconds())
}

// requeue 把发布失败的行退回 pending，失败只记日志：对账会在下一轮再次
// 收敛该行。
func (p *Publisher) requeue(ctx context.Context, row *states.TaskPersistent) {
	if err := p.repo.Requeue(ctx, row.Id); err != nil {
		slog.Error("schedule: failed to requeue task",
			"task_id", row.Id, "resource", row.Resource, "error", err)
	}
}

// signature 组装规则：UUID 按 states.SignatureUUID 协议派生（重派可追踪）；
// Name=任务的 handler 注册名（HandlerName，常规任务即 Resource）；RoutingKey
// 留空，由 dispatch 发布前按 Attribute headers 与集群节点表匹配出目标节点，
// 生成 queue:<resource>/<node> 分节点路由；Args 固定两参（payload JSON 字符串
// + taskID），与 handler 约定签名对应；Headers 由 attributeHeaders 从行生成；
// RetryCount 取任务规格的执行内重试次数。
func (p *Publisher) signature(row *states.TaskPersistent) (*queue.Signature, error) {
	ts, ok := p.reg.Get(row.Resource)
	if !ok {
		return nil, fmt.Errorf("schedule: task resource %s is not registered", row.Resource)
	}
	headers, err := attributeHeaders(row)
	if err != nil {
		return nil, err
	}
	return &queue.Signature{
		UUID: states.SignatureUUID(row.Id),
		Name: ts.HandlerName(),
		Args: []queue.Arg{
			{Type: "string", Value: string(row.Payload)},
			{Type: "int64", Value: row.Id},
		},
		Headers:    headers,
		RetryCount: ts.Retries,
	}, nil
}

// attributeHeaders 把行的目标节点属性写入发布 headers（dispatch 的路由依据，
// 键的含义见 dispatch 的 Attribute 常量）：
//   - AttributeResource 恒写（任务资源）；
//   - AttributeVpcs：行声明了 vpc 约束时携带；
//   - AttributeLabels：行的 label 解析为 键 → 多值（格式见 dispatch.ParseLabels），
//     解析失败即报错，任务不发布；
//   - AttributeClaimedNode / AttributeTargetNode：行的 claimed_node / node，
//     dispatch 选点时优先匹配，claimed 高于 target。注意 claimed_node 目前由
//     ClaimPending 在认领时写入本发布节点的 ID，尚不记录选定的执行节点；
//   - AttributeHashBucket：行的 hash_bucket（0 = 不限，不写），作散列键。
func attributeHeaders(row *states.TaskPersistent) (queue.Headers, error) {
	h := queue.Headers{dispatch.AttributeResource: row.Resource}
	if row.Vpc != "" {
		h[dispatch.AttributeVpcs] = row.Vpc
	}
	labels, err := dispatch.ParseLabels(row.Label)
	if err != nil {
		return nil, fmt.Errorf("schedule: task %d: %w", row.Id, err)
	}
	if len(labels) > 0 {
		h[dispatch.AttributeLabels] = labels
	}
	if row.ClaimedNode != "" {
		h[dispatch.AttributeClaimedNode] = row.ClaimedNode
	}
	if row.Node != "" {
		h[dispatch.AttributeTargetNode] = row.Node
	}
	if row.HashBucket > 0 {
		h[dispatch.AttributeHashBucket] = strconv.Itoa(int(row.HashBucket))
	}
	return h, nil
}
