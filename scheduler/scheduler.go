package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"

	"google.golang.org/protobuf/proto"

	"github.com/improvtrace/stateflux/cluster"
	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/executor"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// ResultSink 同步结果出口（§5.5：与异步结果共用一条 flush 通道）。由 cmd 装配层
// 接到 Collector（collector 实现），调度包不反向依赖归集包。
type ResultSink interface {
	// PushSync 接收同步执行结果（不经执行侧 WAL）。
	PushSync(entry *dispatchv1.ResultEntry)
}

// Options 调度角色构造参数。
type Options struct {
	NodeID       string
	Store        store.Store
	Queue        *queue.Queue
	View         cluster.View
	Pool         *executor.Pool
	Sink         ResultSink
	Cfg          config.SchedulerConfig
	Capabilities map[string][]string // 任务类型 → 必需能力标签（选节点 capabilities 匹配，§5.3/§11）
	Metrics      *obs.Metrics
	Logger       *slog.Logger
}

// Scheduler 调度主循环（§5.3）：双触发（tick 100ms 兜底 + NOTIFY/容量信号驱动）、
// 自适应认领、sync 分发池 / async LPUSH。
type Scheduler struct {
	nodeID  string
	store   store.Store
	q       *queue.Queue
	view    cluster.View
	pool    *executor.Pool
	sink    ResultSink
	cfg     config.SchedulerConfig
	caps    map[string][]string
	metrics *obs.Metrics
	log     *slog.Logger

	syncJobs chan syncJob
	syncWG   sync.WaitGroup
	syncBusy atomic.Int64
}

type syncJob struct {
	msg       *dispatchv1.TaskMessage
	claimedAt time.Time
}

// New 构造调度角色。
func New(opts Options) (*Scheduler, error) {
	if opts.NodeID == "" {
		return nil, errors.New("scheduler: node id is required")
	}
	if opts.Store == nil || opts.Queue == nil || opts.View == nil || opts.Pool == nil {
		return nil, errors.New("scheduler: store/queue/view/pool are required")
	}
	opts.Cfg.ApplyDefaults() // 补默认参数（§10）
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics, _ = obs.New()
	}
	return &Scheduler{
		nodeID:   opts.NodeID,
		store:    opts.Store,
		q:        opts.Queue,
		view:     opts.View,
		pool:     opts.Pool,
		sink:     opts.Sink,
		cfg:      opts.Cfg,
		caps:     opts.Capabilities,
		metrics:  metrics,
		log:      log.With("node", opts.NodeID),
		syncJobs: make(chan syncJob, opts.Cfg.SyncDispatchPool*2),
	}, nil
}

// Run 阻塞运行调度主循环，直到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context, wake <-chan struct{}) {
	// 同步分发协程池（§10：默认 512 goroutine，超出在带缓冲通道排队等待）。
	for i := 0; i < s.cfg.SyncDispatchPool; i++ {
		s.syncWG.Add(1)
		go s.syncWorker(ctx)
	}
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(s.syncJobs) // 停止投递，等池内协程退出
			s.syncWG.Wait()
			return
		case <-ticker.C:
		case <-wake:
		}
		s.tick(ctx)
	}
}

// tick 单轮调度：自适应认领 → 按 exec_mode 分流（§5.3）。
func (s *Scheduler) tick(ctx context.Context) {
	claimN := s.adaptiveClaimSize(ctx)
	if claimN <= 0 {
		return
	}
	claimedAt := time.Now()
	tasks, err := s.store.Claim(ctx, s.nodeID, claimN)
	if err != nil {
		s.log.WarnContext(ctx, "scheduler: claim failed", "err", err)
		return
	}
	if len(tasks) == 0 {
		return
	}
	s.log.DebugContext(ctx, "scheduler: claimed", "count", len(tasks), "want", claimN)

	asyncMsgs := make(map[sdk.Priority][][]byte, 3)
	for _, t := range tasks {
		msg := buildTaskMessage(t, claimedAt)
		if t.ExecMode == sdk.ExecSync {
			// 同步：进分发池（结果经 Sink → 统一 flush 通道；超时/网络错误放弃等待，交对账）。
			s.syncJobs <- syncJob{msg: msg, claimedAt: claimedAt}
			continue
		}
		// 异步：pipeline LPUSH queue:{pri}（§3.2，消息体 = proto TaskMessage 内联完整任务）。
		raw, err := proto.Marshal(msg)
		if err != nil {
			s.log.ErrorContext(ctx, "scheduler: marshal task message", "task_id", t.ID, "err", err)
			continue
		}
		pri := t.Priority.Normalize()
		asyncMsgs[pri] = append(asyncMsgs[pri], raw)
	}
	if len(asyncMsgs) > 0 {
		if err := s.q.Push(ctx, asyncMsgs); err != nil {
			// 投递失败：任务滞留 processing，由对账重置（at-least-once）。
			s.log.ErrorContext(ctx, "scheduler: async push failed", "err", err)
			return
		}
		s.metrics.ClaimToDeliver.Record(ctx, time.Since(claimedAt).Seconds())
	}
}

// adaptiveClaimSize 自适应认领数 = min(claim batch, 各队列剩余容量)（§5.3/§6.4 反压）。
// 只认领当前消化得下的量；claimed-but-not-delivered 滞留超 grace 会被对账重置。
func (s *Scheduler) adaptiveClaimSize(ctx context.Context) int {
	depths, err := s.q.Depths(ctx)
	if err != nil {
		s.log.WarnContext(ctx, "scheduler: queue depth failed", "err", err)
		return 0
	}
	var freeQ int64
	for _, pri := range queue.QueuePriorities {
		free := s.cfg.MaxQueueDepth - depths[pri]
		if free > 0 {
			freeQ += free
		}
	}
	for pri, depth := range depths {
		s.metrics.QueueDepthRecord(ctx, string(pri), depth)
	}
	// 同步闸门余量：分发池空闲（排队量也已含在缓冲内，不再放大认领）。
	syncSlack := int64(s.cfg.SyncDispatchPool) - s.syncBusy.Load()
	claim := int64(s.cfg.ClaimBatch)
	if freeQ < claim {
		claim = freeQ
	}
	if syncSlack < claim {
		claim = syncSlack
	}
	if claim < 0 {
		claim = 0
	}
	return int(claim)
}

// syncWorker 同步分发协程：选节点（capabilities 匹配 + free_slots 最多）→ Execute RPC →
// 结果进统一结果缓冲（§5.3）。
func (s *Scheduler) syncWorker(ctx context.Context) {
	defer s.syncWG.Done()
	for job := range s.syncJobs {
		s.syncBusy.Add(1)
		s.metrics.ClaimToDeliver.Record(ctx, time.Since(job.claimedAt).Seconds())
		s.dispatchSync(ctx, job)
		s.syncBusy.Add(-1)
	}
}

func (s *Scheduler) dispatchSync(ctx context.Context, job syncJob) {
	msg := job.msg
	timeout := time.Duration(msg.TimeoutMs) * time.Millisecond
	node, err := s.selectSyncNode(ctx, msg.Type)
	if err != nil {
		// 无可用执行节点：放弃等待，任务交对账重置（§5.3）。
		s.log.WarnContext(ctx, "scheduler: no executor node for sync task", "task_id", msg.TaskId, "err", err)
		return
	}
	req := &dispatchv1.ExecuteRequest{Task: msg}
	cctx, cancel := context.WithTimeout(ctx, timeout+5*time.Second)
	defer cancel()
	resp, err := s.pool.Execute(cctx, node.Address, req, timeout)
	if err != nil {
		// 结果（或超时/网络错误，放弃等待，交对账）——调度节点不断定执行结果（§5.3）。
		s.log.WarnContext(ctx, "scheduler: sync execute failed",
			"task_id", msg.TaskId, "node", node.ID, "err", err)
		return
	}
	if resp.GetResult() == nil {
		s.log.WarnContext(ctx, "scheduler: sync execute empty result", "task_id", msg.TaskId)
		return
	}
	if s.sink != nil {
		s.sink.PushSync(resp.Result)
	}
}

// selectSyncNode 选节点：capabilities 匹配 + free_slots 最多（§5.3）。
func (s *Scheduler) selectSyncNode(ctx context.Context, taskType string) (cluster.Node, error) {
	nodes := s.view.ExecutorNodes()
	if len(nodes) == 0 {
		return cluster.Node{}, errNoExecutor
	}
	required := s.caps[taskType]
	var best cluster.Node
	bestFree := int64(-1)
	for _, n := range nodes {
		match := true
		for _, c := range required {
			if !n.HasCapability(c) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		free, ok, err := s.q.FreeSlots(ctx, n.ID)
		if err != nil {
			continue
		}
		if !ok {
			free = 0 // 未上报：视为无空闲但仍可参与（避免饿死）
		}
		if free > bestFree {
			best, bestFree = n, free
		}
	}
	if bestFree < 0 {
		return cluster.Node{}, errNoCapableNode
	}
	return best, nil
}

// buildTaskMessage sdk.Task → proto TaskMessage（统一信封，§3.3；内联完整任务）。
func buildTaskMessage(t *sdk.Task, now time.Time) *dispatchv1.TaskMessage {
	return &dispatchv1.TaskMessage{
		TaskId:         t.ID,
		Attempt:        t.Attempts,
		Type:           t.Type,
		Payload:        t.Payload,
		Priority:       priorityToProto(t.Priority),
		TimeoutMs:      t.TimeoutMS,
		DeadlineUnixMs: now.Add(time.Duration(t.TimeoutMS) * time.Millisecond).UnixMilli(),
	}
}

// priorityToProto sdk 枚举 → proto 枚举。
func priorityToProto(p sdk.Priority) dispatchv1.Priority {
	switch p.Normalize() {
	case sdk.PriorityHigh:
		return dispatchv1.Priority_PRIORITY_HIGH
	case sdk.PriorityLow:
		return dispatchv1.Priority_PRIORITY_LOW
	default:
		return dispatchv1.Priority_PRIORITY_NORMAL
	}
}

var (
	errNoExecutor    = errors.New("no executor nodes")
	errNoCapableNode = errors.New("no capable executor node")
)
