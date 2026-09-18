package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/task/codec"
)

// ResultSink 发布一条 ResultEvent（由装配层提供：默认经 gRPC ResultStream 送到调度节点，
// 也可经 Redis 结果通道或直接交给 Collector，§5.4/§5.5）。
type ResultSink func(ctx context.Context, ev *taskv1.ResultEvent) error

// QueueSource 提供「本节点当前应消费的队列」集合（§15.1#4）。生产装配把 coherence 的
// 队列↔节点映射适配为它；返回错误时运行时保留既有订阅（不能因为视图抖动停止消费，§6.1）。
type QueueSource interface {
	AssignedQueues(ctx context.Context, nodeID string) ([]string, error)
}

// RuntimeOptions 是执行运行时装配参数。
type RuntimeOptions struct {
	// Bus 用于订阅异步任务队列（§5.3）。
	Bus *eventbus.EventBus
	// Codec 解码异步载荷（machinery 风格签名帧，§15.1#13）。
	Codec *codec.Codec
	// Handlers 是执行处理器注册表；未命中即返回 failed。
	Handlers *HandlerRegistry
	// Queues 是本节点订阅的逻辑队列名集合（无 QueueSource 时的静态集合，也是视图缺失时的兜底）。
	Queues []string
	// QueueSource 非空时按 coherence 映射动态订阅（§15.1#4）。
	QueueSource QueueSource
	// QueueRefresh 是动态订阅的重算周期；<=0 用 DefaultQueueRefresh。
	QueueRefresh time.Duration
	// NodeID 是本节点 ID（写入 ResultEvent.source）。
	NodeID string
	// ResultSink 发布结果；nil 时 Execute 只写 WAL 不发布。
	ResultSink ResultSink
	// WAL 结果本地日志；nil 时创建进程内默认 WAL。
	WAL WAL
	// Metrics 观测。
	Metrics *obs.Metrics
	// RetryInterval 是 WAL 重放周期；<=0 用 DefaultRetryInterval。
	RetryInterval time.Duration
	// ExecuteTimeout 是 msg.timeout_ms 缺失时的缺省执行预算。
	ExecuteTimeout time.Duration
}

// DefaultRetryInterval 是 WAL 重放周期（§10 ResultStream reconnect 1s 起点）。
const DefaultRetryInterval = time.Second

// DefaultQueueRefresh 是动态队列订阅的重算周期。
const DefaultQueueRefresh = 5 * time.Second

// Runtime 是执行侧运行时（§5.4）：订阅任务队列、执行 handler、先写 WAL 再发布结果。
// 它不写 PG 终态、不决定重试；本地 WAL 与状态都是易失派生数据（§1.2.4）。
type Runtime struct {
	bus      *eventbus.EventBus
	codec    *codec.Codec
	handlers *HandlerRegistry
	queues   []string
	nodeID   string
	sink     ResultSink
	wal      WAL
	metrics  *obs.Metrics
	retry    time.Duration
	timeout  time.Duration

	queueSource  QueueSource
	queueRefresh time.Duration

	mu     sync.Mutex
	subs   map[string]channel.Subscription
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// execMu 保护单次执行的生命周期：Execute 持读锁，Stop 取写锁等待在途执行收尾，
	// 保证关闭 WAL 前没有结果仍在写入（优雅退出的执行侧 drain）。
	execMu sync.RWMutex
}

// NewRuntime 构造执行运行时。
func NewRuntime(opts RuntimeOptions) *Runtime {
	wal := opts.WAL
	if wal == nil {
		wal = NewWAL(DefaultWALMax)
	}
	retry := opts.RetryInterval
	if retry <= 0 {
		retry = DefaultRetryInterval
	}
	timeout := opts.ExecuteTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	refresh := opts.QueueRefresh
	if refresh <= 0 {
		refresh = DefaultQueueRefresh
	}
	return &Runtime{
		bus:          opts.Bus,
		codec:        opts.Codec,
		handlers:     opts.Handlers,
		queues:       append([]string(nil), opts.Queues...),
		queueSource:  opts.QueueSource,
		queueRefresh: refresh,
		nodeID:       opts.NodeID,
		sink:         opts.ResultSink,
		wal:          wal,
		metrics:      opts.Metrics,
		retry:        retry,
		timeout:      timeout,
		subs:         map[string]channel.Subscription{},
	}
}

// WAL 返回运行时使用的结果日志（供水位观测）。
func (r *Runtime) WAL() WAL { return r.wal }

// Start 订阅全部队列并启动 WAL 重放循环；重复 Start 返回错误。
func (r *Runtime) Start(ctx context.Context) error {
	if r.bus == nil {
		return errors.New("worker: nil eventbus")
	}
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return errors.New("worker: runtime already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.mu.Unlock()

	// 任务订阅走 task topic（§5.3）：逻辑 channel 名即队列/topic 名。
	// 初始订阅集来自 coherence 映射；映射不可用时回落到静态配置（§15.1#4）。
	r.subscribeDesired(runCtx)

	r.wg.Add(1)
	go r.retryLoop(runCtx)
	if r.queueSource != nil {
		r.wg.Add(1)
		go r.queueLoop(runCtx)
	}
	return nil
}

// subscribeDesired 计算期望队列集合并与现有订阅对齐。
func (r *Runtime) subscribeDesired(ctx context.Context) {
	desired := append([]string(nil), r.queues...)
	if r.queueSource != nil {
		if queues, err := r.queueSource.AssignedQueues(ctx, r.nodeID); err == nil {
			desired = queues
		}
	}
	r.reconcileSubscriptions(ctx, desired)
}

// reconcileSubscriptions 使订阅集合收敛到 want：新增缺失、关闭多余。
func (r *Runtime) reconcileSubscriptions(ctx context.Context, want []string) {
	wantSet := make(map[string]struct{}, len(want))
	for _, q := range want {
		if q != "" {
			wantSet[q] = struct{}{}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for q, sub := range r.subs {
		if _, ok := wantSet[q]; ok {
			continue
		}
		_ = sub.Close()
		delete(r.subs, q)
	}
	paused := r.wal != nil && r.wal.HighWater()
	for q := range wantSet {
		if _, ok := r.subs[q]; ok {
			continue
		}
		if paused {
			// 高水位：暂停新订阅、继续结果发送（§10、§14.7）。
			continue
		}
		sub, err := r.bus.SubscribeTopic(ctx, channel.Topic(q), r.onEnvelope, eventbus.WithChannel(q))
		if err != nil {
			continue
		}
		r.subs[q] = sub
	}
}

// queueLoop 周期重算订阅集合（coherence 映射变化后自动增减队列）。
func (r *Runtime) queueLoop(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(r.queueRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.subscribeDesired(ctx)
		}
	}
}

// Stop 停止订阅与重放循环；幂等。
func (r *Runtime) Stop() error {
	r.mu.Lock()
	cancel := r.cancel
	subs := r.subs
	r.cancel = nil
	r.subs = map[string]channel.Subscription{}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, sub := range subs {
		_ = sub.Close()
	}
	r.wg.Wait()
	// 停止订阅与循环后，等待在途 Execute 收尾再关闭 WAL：避免结果写入已关闭的日志。
	// handler 受各自执行预算约束，最坏情况由上层关机预算兜底（App.ShutdownTimeout）。
	r.execMu.Lock()
	defer r.execMu.Unlock()
	if r.wal != nil {
		_ = r.wal.Close()
	}
	return nil
}

// SubscribedQueues 返回当前已订阅队列（诊断与测试）。
func (r *Runtime) SubscribedQueues() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.subs))
	for q := range r.subs {
		out = append(out, q)
	}
	sort.Strings(out)
	return out
}

// onEnvelope 处理订阅到的任务载荷：优先按 codec 帧解码，回落到 TaskMessage 原始 protobuf。
func (r *Runtime) onEnvelope(ctx context.Context, env channel.Envelope) error {
	msg, err := r.decode(env.Payload)
	if err != nil {
		return err
	}
	ev := r.Execute(ctx, msg)
	if ev.GetOutcome() == taskv1.Outcome_OUTCOME_UNSET {
		return fmt.Errorf("worker: task %d executed without outcome", msg.GetTaskId())
	}
	return nil
}

// decode 解析任务载荷。
func (r *Runtime) decode(payload []byte) (*taskv1.TaskMessage, error) {
	if r.codec != nil {
		if msg, err := r.codec.Decode(payload); err == nil {
			if tm, ok := msg.(*taskv1.TaskMessage); ok {
				return tm, nil
			}
		}
	}
	tm := &taskv1.TaskMessage{}
	if err := proto.Unmarshal(payload, tm); err != nil {
		return nil, fmt.Errorf("worker: decode task message: %w", err)
	}
	return tm, nil
}

// Execute 同步执行一个任务消息并发布结果（供 ExecutorService.Execute 与订阅路径共用）。
func (r *Runtime) Execute(ctx context.Context, msg *taskv1.TaskMessage) *taskv1.ResultEvent {
	// 在途执行登记：Stop 取写锁时等待这里释放，确保执行收尾后才关闭 WAL。
	r.execMu.RLock()
	defer r.execMu.RUnlock()

	start := time.Now()
	ev := &taskv1.ResultEvent{
		TaskId:         msg.GetTaskId(),
		Attempt:        msg.GetAttempt(),
		Source:         r.nodeID,
		Type:           msg.GetType(),
		Operator:       msg.GetOperator(),
		FinishedUnixMs: start.UnixMilli(),
	}
	handler, ok := r.resolve(msg.GetType(), msg.GetOperator())
	if !ok {
		ev.Outcome = taskv1.Outcome_OUTCOME_FAILED
		ev.Error = fmt.Sprintf("worker: no handler for %s:%s", msg.GetType(), msg.GetOperator())
		r.recordAndPublish(ctx, ev)
		return ev
	}
	timeout := r.timeout
	if msg.GetTimeoutMs() > 0 {
		timeout = time.Duration(msg.GetTimeoutMs()) * time.Millisecond
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	res, err := handler.Handle(execCtx, msg)
	if r.metrics != nil {
		r.metrics.HandlerDurationRecord(ctx, time.Since(start).Seconds(), msg.GetType())
	}
	switch {
	case err != nil:
		ev.Outcome = taskv1.Outcome_OUTCOME_FAILED
		ev.Error = err.Error()
	case res.Outcome == taskv1.Outcome_OUTCOME_UNSET:
		ev.Outcome = taskv1.Outcome_OUTCOME_FAILED
		ev.Error = "worker: handler returned unset outcome"
	default:
		ev.Outcome = res.Outcome
		ev.Result = res.Payload
		ev.Error = res.Error
	}
	ev.FinishedUnixMs = time.Now().UnixMilli()
	r.recordAndPublish(ctx, ev)
	return ev
}

func (r *Runtime) resolve(taskType, operator string) (Handler, bool) {
	if r.handlers == nil {
		return nil, false
	}
	return r.handlers.Resolve(taskType, operator)
}

// recordAndPublish 先写 WAL 再发布，发布成功即确认（§5.4）。
func (r *Runtime) recordAndPublish(ctx context.Context, ev *taskv1.ResultEvent) {
	r.wal.Add(ev)
	if r.sink == nil {
		return
	}
	if err := r.sink(ctx, ev); err == nil {
		r.wal.Ack(ev.GetTaskId(), ev.GetAttempt())
	}
	r.recordBacklog(ctx)
}

// retryLoop 周期重放未确认结果（断线重连语义，§5.4）。
func (r *Runtime) retryLoop(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(r.retry)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.replay(ctx)
		}
	}
}

func (r *Runtime) replay(ctx context.Context) {
	if r.sink == nil {
		return
	}
	for _, ev := range r.wal.Pending() {
		if ctx.Err() != nil {
			return
		}
		if err := r.sink(ctx, ev); err == nil {
			r.wal.Ack(ev.GetTaskId(), ev.GetAttempt())
		}
	}
	r.recordBacklog(ctx)
}

func (r *Runtime) recordBacklog(ctx context.Context) {
	if r.metrics == nil {
		return
	}
	r.metrics.WALBacklogRecord(ctx, int64(r.wal.Len()), r.wal.Bytes())
}
