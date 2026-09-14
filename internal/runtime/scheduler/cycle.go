package scheduler

import (
	"context"
	"errors"
	"time"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/task/dispatch"
)

// Ledger 是调度周期所需的最小账本面（由 repository.Store 实现，§5.2/§6.2）。
type Ledger interface {
	// Promote 批量 pending → schedulable。
	Promote(ctx context.Context, limit int) (int, error)
	// Claim 原子认领 schedulable → processing（attempts +1）。
	Claim(ctx context.Context, limit int) ([]repository.Claimed, error)
	// GetPayload 读取在途任务 payload（构造 TaskMessage，§3.3）。
	GetPayload(ctx context.Context, taskID int64) ([]byte, error)
	// DeadLetter 把超限任务写为 dead（R3）。
	DeadLetter(ctx context.Context, taskID int64) (bool, error)
}

// CycleOptions 是 LedgerCycle 装配参数。
type CycleOptions struct {
	Ledger       Ledger
	Dispatcher   *dispatch.Dispatcher
	Bus          *eventbus.EventBus
	NodeID       string
	PromoteLimit int
	ClaimBatch   int
	Config       config.Dispatch
	Metrics      *obs.Metrics
}

// LedgerCycle 是默认调度周期（§5.2/§5.3）：晋升 → 认领 → 分发。
type LedgerCycle struct {
	ledger       Ledger
	dispatcher   *dispatch.Dispatcher
	bus          *eventbus.EventBus
	nodeID       string
	promoteLimit int
	claimBatch   int
	cfg          config.Dispatch
	metrics      *obs.Metrics
}

// NewCycle 构造默认调度周期。
func NewCycle(opts CycleOptions) *LedgerCycle {
	promote := opts.PromoteLimit
	if promote <= 0 {
		promote = opts.ClaimBatch
	}
	if promote <= 0 {
		promote = 500
	}
	claim := opts.ClaimBatch
	if claim <= 0 {
		claim = 500
	}
	return &LedgerCycle{
		ledger:       opts.Ledger,
		dispatcher:   opts.Dispatcher,
		bus:          opts.Bus,
		nodeID:       opts.NodeID,
		promoteLimit: promote,
		claimBatch:   claim,
		cfg:          opts.Config,
		metrics:      opts.Metrics,
	}
}

// RunOnce 执行一轮：晋升、认领、逐条分发。单条分发失败不终止整轮——任务留在
// processing，由 R1 在 grace 后重置重投（§5.3、§6.1）。
func (c *LedgerCycle) RunOnce(ctx context.Context) (CycleResult, error) {
	var res CycleResult
	if c.ledger == nil || c.dispatcher == nil {
		return res, errors.New("runtime/scheduler: cycle missing ledger or dispatcher")
	}
	promoted, err := c.ledger.Promote(ctx, c.promoteLimit)
	if err != nil {
		return res, err
	}
	res.Promoted = promoted

	claimed, err := c.ledger.Claim(ctx, c.claimBatch)
	if err != nil {
		return res, err
	}
	res.Claimed = len(claimed)

	for _, p := range claimed {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		// R3：超过最大认领次数直接写死信，不再分发（§6.2）。
		if p.Attempt > int64(p.MaxAttempts) {
			if _, derr := c.ledger.DeadLetter(ctx, p.TaskID); derr == nil && c.metrics != nil {
				c.metrics.ResetCount(ctx, "dead_letter", 1)
			}
			res.Skipped++
			continue
		}
		msg, merr := c.message(ctx, p)
		if merr != nil {
			res.Skipped++
			continue
		}
		delivery, semantics := c.route(p)
		out, derr := c.dispatcher.Deliver(ctx, dispatch.Request{
			Queue:         p.Channel,
			Message:       msg,
			Delivery:      delivery,
			Semantics:     semantics,
			CorrelationID: p.ClaimedNode,
		})
		if derr != nil || out == nil || !out.Accepted {
			res.Skipped++
			continue
		}
		res.Dispatched++
	}
	return res, nil
}

// message 把在途行投影为不可变 TaskMessage（§3.3）。
func (c *LedgerCycle) message(ctx context.Context, p repository.Processing) (*taskv1.TaskMessage, error) {
	payload, err := c.ledger.GetPayload(ctx, p.TaskID)
	if err != nil {
		return nil, err
	}
	timeout := p.TimeoutMs
	if timeout <= 0 {
		timeout = 60_000
	}
	return &taskv1.TaskMessage{
		TaskId:         p.TaskID,
		Attempt:        p.Attempt,
		Type:           p.Type,
		Operator:       p.Operator,
		Payload:        payload,
		Priority:       int32(p.Priority),
		TimeoutMs:      timeout,
		DeadlineUnixMs: time.Now().Add(time.Duration(timeout) * time.Millisecond).UnixMilli(),
		Channel:        p.Channel,
		Vpc:            p.Vpc,
		Node:           p.Node,
		Label:          p.Label,
		HashBucket:     int32(p.HashBucket),
		IdempotencyKey: p.IdempotencyKey,
		OriginNode:     c.nodeID,
		CreatedUnixMs:  p.CreatedAt.UnixMilli(),
	}, nil
}

// route 推导投递形态与语义：同步/异步由被解析 channel 的 Capabilities 决定（§14.1），
// 语义默认取配置（节点间转发对已认领任务按 at_least_once 兜底）。
func (c *LedgerCycle) route(p repository.Processing) (config.Delivery, config.Semantics) {
	delivery := c.cfg.DefaultDelivery
	if c.bus != nil {
		if ch, err := c.bus.Resolve(p.Channel); err == nil {
			if ch.Capabilities().RequestReply {
				delivery = config.DeliverySyncRPC
			} else if ch.Capabilities().Subscribe {
				delivery = config.DeliveryRedisQueue
			}
		}
	}
	semantics := c.cfg.DefaultSemantics
	if semantics == "" {
		semantics = config.AtLeastOnce
	}
	return delivery, semantics
}
