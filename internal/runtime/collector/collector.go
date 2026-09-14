// Package collector 实现阶段 4 结果归集（§5.5、§8）：订阅 EventBus 的 result 事件
// （同步 RPC 的响应也适配为同一 ResultEvent 后从该入口进入），每条结果经一次 PG 事务
// 完成终态搬移并写墓碑；重复事件因 attempt fence 与墓碑主键冲突被拒，无副作用。
package collector

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
	"github.com/improvtrace/stateflux/internal/obs"
)

// Ledger 是归集所需的最小账本面（由 repository.Store 实现，§5.5/§6.2）。
type Ledger interface {
	// Complete 校验 attempt 后写终态；stale/重复返回 false。
	Complete(ctx context.Context, req repository.CompleteRequest) (bool, error)
	// GetProcessing 读取在途行（判断重试次数，§6.2 R3）。
	GetProcessing(ctx context.Context, taskID int64) (*repository.Processing, error)
	// Requeue 可重试失败：回到 schedulable，不增加 attempts（§5.5）。
	Requeue(ctx context.Context, taskID int64) (bool, error)
	// DeadLetter R3：attempts 用尽写 dead。
	DeadLetter(ctx context.Context, taskID int64) (bool, error)
}

// Collector 把 ResultEvent 归集为终态。
type Collector struct {
	ledger     Ledger
	metrics    *obs.Metrics
	onTerminal func(ctx context.Context, req repository.CompleteRequest)
}

// Options 是 Collector 装配参数。
type Options struct {
	Ledger  Ledger
	Metrics *obs.Metrics
	// OnTerminal 在终态提交成功后回调（如回调派生，§5.6）；可空。
	OnTerminal func(ctx context.Context, req repository.CompleteRequest)
}

// New 构造 Collector。
func New(opts Options) *Collector {
	return &Collector{ledger: opts.Ledger, metrics: opts.Metrics, onTerminal: opts.OnTerminal}
}

// Consume 归集一条结果，实现 biz.ResultConsumer。
func (c *Collector) Consume(ctx context.Context, ev *taskv1.ResultEvent) error {
	if c.ledger == nil {
		return errors.New("runtime/collector: nil ledger")
	}
	if ev == nil || ev.GetTaskId() == 0 {
		return errors.New("runtime/collector: invalid result event")
	}
	outcome := outcomeOf(ev.GetOutcome())

	// 失败结果先判断重试预算：用尽即死信，否则回 schedulable（不增加 attempts，§5.5/§6.2）。
	if outcome == schema.OutcomeFailed {
		if handled, err := c.retry(ctx, ev); err != nil {
			return err
		} else if handled {
			return nil
		}
	}

	completedAt := time.Now()
	if ms := ev.GetFinishedUnixMs(); ms > 0 {
		completedAt = time.UnixMilli(ms)
	}
	ok, err := c.ledger.Complete(ctx, repository.CompleteRequest{
		TaskID:      ev.GetTaskId(),
		Attempt:     ev.GetAttempt(),
		Outcome:     outcome,
		Result:      jsonOrRaw(ev.GetResult()),
		Error:       ev.GetError(),
		CompletedAt: completedAt,
	})
	if err != nil {
		return err
	}
	c.record(ctx, outcome, ok)
	if ok && c.onTerminal != nil {
		c.onTerminal(ctx, repository.CompleteRequest{
			TaskID:      ev.GetTaskId(),
			Attempt:     ev.GetAttempt(),
			Outcome:     outcome,
			Result:      jsonOrRaw(ev.GetResult()),
			Error:       ev.GetError(),
			CompletedAt: completedAt,
		})
	}
	return nil
}

// retry 处理可重试失败：返回 handled=true 表示已按重试/死信路径处理，无需再写终态。
func (c *Collector) retry(ctx context.Context, ev *taskv1.ResultEvent) (bool, error) {
	p, err := c.ledger.GetProcessing(ctx, ev.GetTaskId())
	if err != nil {
		// 在途行不存在（可能已被 R1 重置或重复归集）：交由 Complete 走 stale 路径。
		return false, nil
	}
	if p.Attempt != ev.GetAttempt() {
		// 陈旧结果：attempt fence 拒绝，无副作用（§6.2）。
		return true, nil
	}
	if p.MaxAttempts > 0 && p.Attempt >= int64(p.MaxAttempts) {
		if _, derr := c.ledger.DeadLetter(ctx, ev.GetTaskId()); derr != nil {
			return true, derr
		}
		c.record(ctx, schema.OutcomeDead, true)
		return true, nil
	}
	if _, rerr := c.ledger.Requeue(ctx, ev.GetTaskId()); rerr != nil {
		return true, rerr
	}
	if c.metrics != nil {
		c.metrics.ResetCount(ctx, "retry", 1)
	}
	return true, nil
}

func (c *Collector) record(ctx context.Context, outcome schema.Outcome, accepted bool) {
	if c.metrics == nil {
		return
	}
	if !accepted {
		c.metrics.CollectorResultRecord(ctx, "stale", 1)
		return
	}
	c.metrics.CollectorResultRecord(ctx, outcome.String(), 1)
}

// jsonOrRaw 把结果字节规范化为 jsonb 可写形态：合法 JSON 原样保留，否则包成 JSON 字符串。
func jsonOrRaw(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, err := json.Marshal(string(b))
	if err != nil {
		return nil
	}
	return json.RawMessage(quoted)
}

func outcomeOf(o taskv1.Outcome) schema.Outcome {
	switch o {
	case taskv1.Outcome_OUTCOME_SUCCEEDED:
		return schema.OutcomeSucceeded
	case taskv1.Outcome_OUTCOME_DEAD:
		return schema.OutcomeDead
	default:
		return schema.OutcomeFailed
	}
}
