// Package collector 实现归集角色（§5.5，阶段 4，与调度节点同进程）：
// Collect 拉取 → 终态事务（processing→completed 搬移，payload 合并 + task_results 写入 +
// 回调派生，同一事务）→ 终态墓碑 → Ack。pull 模型 + Ack 两段式：WAL 在 Ack 前不截断、
// Collect 重入安全、PG 回写 attempt 幂等——三者共同保证结果至多落一次库、绝不丢。
package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"

	"github.com/improvtrace/stateflux/cluster"
	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/executor"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/scheduler"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// Collector 归集角色。实现 scheduler.ResultSink：同步任务的结果由调度节点直接进
// 统一结果缓冲，与异步结果共用一条 flush 通道（§5.5）。
type Collector struct {
	store   store.Store
	q       *queue.Queue
	view    cluster.View
	pool    *executor.Pool
	buf     *scheduler.Buffer
	cfg     config.CollectorConfig
	metrics *obs.Metrics
	log     *slog.Logger

	// flushMu 串行化终态批处理（单写路径语义：同一时刻只有一个 Finalize 在跑）。
	flushMu sync.Mutex
}

// Options 归集角色构造参数。
type Options struct {
	Store   store.Store
	Queue   *queue.Queue
	View    cluster.View
	Pool    *executor.Pool
	Cfg     config.CollectorConfig
	Metrics *obs.Metrics
	Logger  *slog.Logger
}

// New 构造归集角色。
func New(opts Options) *Collector {
	opts.Cfg.ApplyDefaults()
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics, _ = obs.New()
	}
	return &Collector{
		store:   opts.Store,
		q:       opts.Queue,
		view:    opts.View,
		pool:    opts.Pool,
		buf:     scheduler.NewBuffer(opts.Cfg.FlushCount),
		cfg:     opts.Cfg,
		metrics: metrics,
		log:     log,
	}
}

// PushSync 实现 scheduler.ResultSink：同步执行结果直接进统一结果缓冲（§5.4/§5.5）。
func (c *Collector) PushSync(entry *dispatchv1.ResultEntry) {
	if entry == nil {
		return
	}
	c.buf.Add(&scheduler.BufferedResult{Entry: entry, Kind: scheduler.ResultSync})
}

// Run 阻塞运行：拉取循环（Collect）+ flush 循环（终态事务 → 墓碑 → Ack），直到 ctx 取消。
func (c *Collector) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.pullLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.flushLoop(ctx)
	}()
	wg.Wait()
}

// pullLoop 拉取周期（默认 500ms，§10）：并行拉取各执行节点。
func (c *Collector) pullLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.PullInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		nodes := c.view.ExecutorNodes()
		var wg sync.WaitGroup
		for _, node := range nodes {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				resp, err := c.pool.Collect(ctx, addr, c.cfg.PullBatch)
				if err != nil {
					c.log.DebugContext(ctx, "collector: pull failed", "node", addr, "err", err)
					return
				}
				results := resp.GetResults()
				if len(results) == 0 {
					return
				}
				items := make([]*scheduler.BufferedResult, 0, len(results))
				for _, entry := range results {
					items = append(items, &scheduler.BufferedResult{
						Entry:      entry,
						Kind:       scheduler.ResultAsync,
						SourceNode: addr,
					})
				}
				c.buf.Add(items...)
			}(node.Address)
		}
		wg.Wait()
	}
}

// flushLoop 缓冲双阈值（200 条或 1s）驱动终态批处理（§5.5）。
func (c *Collector) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// 退出前尽力 flush（结果已 WAL 保留，不丢；尽力收敛减少重跑窗口）。
			c.flush(ctx)
			return
		case <-ticker.C:
		case <-c.buf.Signal():
		}
		c.flush(ctx)
	}
}

// flush 终态批处理：路由 → Finalize（终态搬移/重试，同一事务）→ 墓碑 → Ack。
func (c *Collector) flush(ctx context.Context) {
	items := c.buf.Drain(0)
	if len(items) == 0 {
		return
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	now := time.Now()
	entries := make([]store.TerminalEntry, 0, len(items))
	// Ack 组：按来源节点归组（仅异步结果需要 Ack——同步结果不经执行侧 WAL）。
	ackByNode := make(map[string][]int64)
	seen := make(map[int64]struct{}, len(items))
	for _, item := range items {
		e := item.Entry
		if e == nil {
			continue
		}
		if _, dup := seen[e.TaskId]; dup {
			continue // 重拉窗口内的重复条目：终态写入幂等，这里直接去重
		}
		seen[e.TaskId] = struct{}{}
		if item.Kind == scheduler.ResultAsync {
			ackByNode[item.SourceNode] = append(ackByNode[item.SourceNode], e.TaskId)
		}
		outcome := sdk.Outcome(e.Outcome)
		entry := store.TerminalEntry{
			TaskID:      e.TaskId,
			Attempt:     e.Attempt,
			CompletedAt: time.UnixMilli(e.CompletedAtUnixMs),
			Error:       e.Error,
		}
		switch {
		case outcome == sdk.OutcomeSucceeded:
			entry.Action = store.TerminalActionTerminal
			entry.Outcome = sdk.OutcomeSucceeded
			entry.Result = e.Result
		case e.NonRetryable:
			// 业务标记不可重试：终态 failed，不进重试路径（§5.5）。
			entry.Action = store.TerminalActionTerminal
			entry.Outcome = sdk.OutcomeFailed
		default:
			// 可重试失败：挪回 schedulable（退避 full jitter，§6.3）；
			// attempts 耗尽由 store 路由 dead（R3 同款）。
			entry.Action = store.TerminalActionRetry
			entry.RetryRunAt = now.Add(sdk.Backoff(e.Attempt))
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return
	}

	report, err := c.store.Finalize(ctx, entries)
	if err != nil {
		// 终态事务失败：结果保留在执行侧 WAL / 同步结果已消费——WAL 部分由重拉兜底（§6.1）。
		c.log.ErrorContext(ctx, "collector: finalize failed", "count", len(entries), "err", err)
		return
	}

	// 终态计数 + 归集延迟（结果完成 → 终态落库）；墓碑只覆盖真正终态化的任务——
	// 重试路由的任务会再次投递，写墓碑会把新副本挡在注册防线之外（§6.2）。
	var terminalized []int64
	for _, entry := range entries {
		if report.Applied[entry.TaskID] != store.EntryStatusApplied {
			continue
		}
		if entry.Action == store.TerminalActionTerminal {
			terminalized = append(terminalized, entry.TaskID)
			c.metrics.TerminalCount(ctx, string(entry.Outcome))
			latency := now.Sub(entry.CompletedAt).Seconds()
			if latency < 0 {
				latency = 0
			}
			c.metrics.CollectLatency.Record(ctx, latency)
		}
	}

	// 终态墓碑（§6.2）：TTL ≥ grace，自动回收。
	if len(terminalized) > 0 {
		if err := c.q.SetTombstones(ctx, terminalized, c.cfg.TombstoneTTL); err != nil {
			c.log.WarnContext(ctx, "collector: set tombstones failed", "err", err)
		}
	}

	// Ack（§5.5）：结果已被本框架消费（无论终态化还是 attempt 不匹配跳过），
	// 执行侧推进 WAL 水位并移出 inprocess；Ack 丢失无害（重拉 → PG 幂等 → 重新 Ack）。
	for addr, ids := range ackByNode {
		if err := c.pool.Ack(ctx, addr, ids); err != nil {
			c.log.WarnContext(ctx, "collector: ack failed", "node", addr, "count", len(ids), "err", err)
		}
	}
	if len(report.Derived) > 0 {
		c.log.InfoContext(ctx, "collector: callbacks derived", "count", len(report.Derived))
	}
}
