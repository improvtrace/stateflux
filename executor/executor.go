// Package executor 是执行引擎（§5.4，阶段 3）：消费循环 / 租约 / 结果 WAL / gRPC server。
// 执行侧无状态（§1.2.7）：不做任何调度决策，只消费任务、执行 handler、缓冲结果——
// 本地结果 WAL 与容量上报是易失的派生数据而非权威状态；崩溃无损失，扩容就是加进程。
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"

	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/sdk"
)

// Options 执行角色构造参数。
type Options struct {
	NodeID   string
	Registry *sdk.Registry
	Queue    *queue.Queue
	WAL      *WAL
	// Concurrency 并发槽（free_slots 上报依据）。
	Concurrency int
	// LeaseTTL / LeaseRenewInterval 租约与续约周期。
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	// BRPOPTimeout 消费阻塞超时（影响退出延迟）。
	BRPOPTimeout time.Duration
	// CapacityReportInterval free_slots 上报周期。
	CapacityReportInterval time.Duration
	Metrics                *obs.Metrics
	Logger                 *slog.Logger
}

// Executor 执行角色（所有节点常驻，§2.1）。
type Executor struct {
	nodeID   string
	registry *sdk.Registry
	q        *queue.Queue
	wal      *WAL
	leases   *leaseManager
	metrics  *obs.Metrics
	log      *slog.Logger

	concurrency int
	leaseTTL    time.Duration
	brpopWait   time.Duration
	reportEvery time.Duration

	jobs     chan *dispatchv1.TaskMessage
	inflight atomic.Int64
	stopped  atomic.Bool
}

// New 构造执行角色。
func New(opts Options) (*Executor, error) {
	if opts.NodeID == "" {
		return nil, fmt.Errorf("executor: node id is required")
	}
	if opts.Registry == nil {
		return nil, fmt.Errorf("executor: registry is required")
	}
	if opts.Queue == nil {
		return nil, fmt.Errorf("executor: queue is required")
	}
	if opts.WAL == nil {
		return nil, fmt.Errorf("executor: wal is required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 64
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.LeaseRenewInterval <= 0 {
		opts.LeaseRenewInterval = 10 * time.Second
	}
	if opts.BRPOPTimeout <= 0 {
		opts.BRPOPTimeout = time.Second
	}
	if opts.CapacityReportInterval <= 0 {
		opts.CapacityReportInterval = 5 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		var err error
		metrics, err = obs.New()
		if err != nil {
			return nil, fmt.Errorf("executor: metrics: %w", err)
		}
	}
	return &Executor{
		nodeID:      opts.NodeID,
		registry:    opts.Registry,
		q:           opts.Queue,
		wal:         opts.WAL,
		leases:      newLeaseManager(opts.Queue, opts.NodeID, opts.LeaseTTL, opts.LeaseRenewInterval, log),
		metrics:     metrics,
		log:         log.With("node", opts.NodeID),
		concurrency: opts.Concurrency,
		leaseTTL:    opts.LeaseTTL,
		brpopWait:   opts.BRPOPTimeout,
		reportEvery: opts.CapacityReportInterval,
		jobs:        make(chan *dispatchv1.TaskMessage, opts.Concurrency),
	}, nil
}

// WAL 暴露结果 WAL（Collector gRPC server 使用）。
func (e *Executor) WAL() *WAL { return e.wal }

// Run 阻塞运行：worker 池 + 消费循环 + 容量上报，直到 ctx 取消（优雅退出）。
func (e *Executor) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	// worker 池。
	for i := 0; i < e.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range e.jobs {
				e.executeAsync(ctx, msg)
				e.inflight.Add(-1)
			}
		}()
	}
	// 容量上报。
	reportCtx, cancelReport := context.WithCancel(ctx)
	defer cancelReport()
	go e.reportCapacityLoop(reportCtx)

	// 消费循环。
	for {
		if ctx.Err() != nil {
			break
		}
		// 高水位反压（§5.4）：WAL 积压超阈值暂停 BRPOP，队列深度上升反馈调度侧缩 claim。
		if e.wal.OverHighWatermark() {
			n, b := e.wal.Backlog()
			e.log.Warn("executor: wal over high watermark, pause brpop", "entries", n, "bytes", b)
			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		keys := make([]string, 0, len(queue.QueuePriorities))
		for _, pri := range queue.QueuePriorities {
			keys = append(keys, queue.QueueKey(pri))
		}
		res, err := e.q.Client().BRPop(ctx, e.brpopWait, keys...).Result()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, context.Canceled) {
				break
			}
			e.log.Warn("executor: brpop error", "err", err)
			continue
		}
		if len(res) < 2 {
			continue
		}
		msg := &dispatchv1.TaskMessage{}
		if err := protoUnmarshal([]byte(res[1]), msg); err != nil {
			e.log.Warn("executor: decode task message failed", "err", err)
			continue
		}
		// 先注册 inprocess（BRPOP 后立即，缩小 §4 崩溃窗口）；被拒 = 陈旧副本或他人持有，无害丢弃。
		reg, err := e.q.Register(ctx, msg.TaskId, msg.Attempt, e.nodeID, e.leaseTTL)
		if err != nil {
			e.log.Warn("executor: register inprocess failed", "task_id", msg.TaskId, "err", err)
			continue
		}
		if !reg.OK {
			e.metrics.RejectCount(ctx, string(reg.RejectOf))
			e.log.Info("executor: stale copy rejected", "task_id", msg.TaskId, "reason", reg.RejectOf)
			continue
		}
		e.leases.Start(msg.TaskId, msg.Attempt)
		e.inflight.Add(1)
		select {
		case e.jobs <- msg:
		case <-ctx.Done():
			e.inflight.Add(-1)
			break
		}
	}
	// 优雅退出：等在途任务执行完（不强制杀死执行到一半的任务，§13）。
	close(e.jobs)
	wg.Wait()
	e.leases.StopAll()
	return nil
}

// reportCapacityLoop 周期上报 free_slots（§5.4）。
func (e *Executor) reportCapacityLoop(ctx context.Context) {
	ticker := time.NewTicker(e.reportEvery)
	defer ticker.Stop()
	for {
		if err := e.q.ReportCapacity(ctx, e.nodeID, int64(e.concurrency)-e.inflight.Load(), 2*e.leaseTTL); err != nil {
			e.log.Debug("executor: report capacity failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// executeAsync 异步任务执行（§5.4）：handler（ctx 带超时）→ 结果写入执行侧结果 WAL →
// 等归集 Ack 后由 server 层移出 inprocess。
func (e *Executor) executeAsync(ctx context.Context, msg *dispatchv1.TaskMessage) {
	entry := e.runHandler(ctx, msg)
	if err := e.wal.Append(entry); err != nil {
		// 落 WAL 失败：结果丢失 → 该次执行视同未完成，交对账重跑（at-least-once）。
		e.log.Error("executor: wal append failed, result lost until reconcile",
			"task_id", msg.TaskId, "err", err)
	}
}

// runHandler 执行 handler 并产出结果条目（sync/async 共用）。
func (e *Executor) runHandler(ctx context.Context, msg *dispatchv1.TaskMessage) *dispatchv1.ResultEntry {
	task := taskFromMessage(msg)
	entry := &dispatchv1.ResultEntry{
		TaskId:  msg.TaskId,
		Attempt: msg.Attempt,
	}
	start := time.Now()
	handler, err := e.registry.Lookup(msg.Type)
	if err != nil {
		entry.Outcome = string(sdk.OutcomeFailed)
		entry.Error = err.Error()
		entry.CompletedAtUnixMs = time.Now().UnixMilli()
		e.metrics.HandlerDuration.Record(ctx, time.Since(start).Seconds())
		return entry
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(msg.TimeoutMs)*time.Millisecond)
	defer cancel()
	result, herr := handler.Execute(runCtx, task)
	e.metrics.HandlerDuration.Record(runCtx, time.Since(start).Seconds(), measureType(msg.Type))
	entry.CompletedAtUnixMs = time.Now().UnixMilli()
	if herr != nil {
		entry.Outcome = string(sdk.OutcomeFailed)
		entry.Error = herr.Error()
		entry.NonRetryable = sdk.IsNonRetryable(herr)
		return entry
	}
	entry.Outcome = string(sdk.OutcomeSucceeded)
	entry.Result = result
	return entry
}

// taskFromMessage proto TaskMessage → sdk.Task（Handler 入参）。
func taskFromMessage(msg *dispatchv1.TaskMessage) *sdk.Task {
	return &sdk.Task{
		ID:        msg.TaskId,
		Type:      msg.Type,
		Payload:   msg.Payload,
		Priority:  sdk.Priority(msg.Priority.String()),
		Attempts:  msg.Attempt,
		TimeoutMS: msg.TimeoutMs,
	}
}
