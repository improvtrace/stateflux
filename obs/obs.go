// Package obs 集中定义 §6.5 最小指标集（单一采集标准 = OpenTelemetry）。
// 各角色包只依赖注入的 *obs.Metrics，不感知导出协议；未启用 OTel 时 instruments 为
// no-op，零开销。标签纪律：只允许低基数标签（priority、type 白名单、node_id、outcome、
// reason）；task_id/业务 key 禁止入标签。
//
// 水位型指标（queue.depth 等）设计稿定为 ObservableGauge；实现采用同步 Int64Gauge——
// 采集点位收敛在既有写路径（§6.5），在测量点直接记录，语义相同且免去回调装配。
package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics stateflux 全框架指标集（§6.5 最小指标集）。
type Metrics struct {
	meter metric.Meter

	// stateflux.queue.depth —— 调度：各就绪队列深度（LLEN）。
	QueueDepth metric.Int64Gauge
	// stateflux.inprocess.size —— 调度/对账：共识集合大小（ZCARD）。
	InprocessSize metric.Int64Gauge
	// stateflux.claim.to_deliver_latency —— 调度：claim→投递延迟。
	ClaimToDeliver metric.Float64Histogram
	// stateflux.collect.latency —— Collector：结果完成→终态落库（归集延迟）。
	CollectLatency metric.Float64Histogram
	// stateflux.reconcile.resets —— 对账：R1 重置次数（核心健康信号）。
	ReconcileResets metric.Int64Counter
	// stateflux.inprocess.rejects —— queue：注册拒绝数（§6.2 双防线命中）。
	InprocessRejects metric.Int64Counter
	// stateflux.tasks.terminal —— Collector：终态计数（succeeded/failed/dead）。
	TasksTerminal metric.Int64Counter
	// stateflux.wal.backlog —— Executor：WAL 未 Ack 条数/字节数（反压水位）。
	WALBacklogEntries metric.Int64Gauge
	WALBacklogBytes   metric.Int64Gauge
	// stateflux.executor.free_slots —— Executor：容量上报。
	FreeSlots metric.Int64Gauge
	// stateflux.handler.duration —— Executor：handler 耗时。
	HandlerDuration metric.Float64Histogram
	// stateflux.stage.size —— 各阶段表规模（可选观测）。
	StageSize metric.Int64Gauge
}

// New 创建指标集（otel.Meter 经 config 装配的全局 MeterProvider 解析；未启用 OTel 时为 no-op）。
func New() (*Metrics, error) {
	meter := otel.Meter("stateflux")
	m := &Metrics{meter: meter}
	instruments := []struct {
		mk func(metric.Meter) error
	}{
		{func(mm metric.Meter) (err error) { m.QueueDepth, err = mm.Int64Gauge("stateflux.queue.depth"); return }},
		{func(mm metric.Meter) (err error) {
			m.InprocessSize, err = mm.Int64Gauge("stateflux.inprocess.size")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.ClaimToDeliver, err = mm.Float64Histogram("stateflux.claim.to_deliver_latency", metric.WithUnit("s"))
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.CollectLatency, err = mm.Float64Histogram("stateflux.collect.latency", metric.WithUnit("s"))
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.ReconcileResets, err = mm.Int64Counter("stateflux.reconcile.resets")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.InprocessRejects, err = mm.Int64Counter("stateflux.inprocess.rejects")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.TasksTerminal, err = mm.Int64Counter("stateflux.tasks.terminal")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.WALBacklogEntries, err = mm.Int64Gauge("stateflux.wal.backlog.entries")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.WALBacklogBytes, err = mm.Int64Gauge("stateflux.wal.backlog.bytes")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.FreeSlots, err = mm.Int64Gauge("stateflux.executor.free_slots")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.HandlerDuration, err = mm.Float64Histogram("stateflux.handler.duration", metric.WithUnit("s"))
			return
		}},
		{func(mm metric.Meter) (err error) { m.StageSize, err = mm.Int64Gauge("stateflux.stage.size"); return }},
	}
	for _, ins := range instruments {
		if err := ins.mk(meter); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// ---- 录制辅助（低基数标签在此收敛） ----

// QueueDepthRecord 各优先级队列深度。
func (m *Metrics) QueueDepthRecord(ctx context.Context, priority string, v int64) {
	if m.QueueDepth != nil {
		m.QueueDepth.Record(ctx, v, metric.WithAttributes(attribute.String("priority", priority)))
	}
}

// InprocessSizeRecord 共识集合大小。
func (m *Metrics) InprocessSizeRecord(ctx context.Context, v int64) {
	if m.InprocessSize != nil {
		m.InprocessSize.Record(ctx, v)
	}
}

// WALBacklogRecord WAL 未 Ack 条数/字节数。
func (m *Metrics) WALBacklogRecord(ctx context.Context, entries, bytes int64) {
	if m.WALBacklogEntries != nil {
		m.WALBacklogEntries.Record(ctx, entries)
	}
	if m.WALBacklogBytes != nil {
		m.WALBacklogBytes.Record(ctx, bytes)
	}
}

// FreeSlotsRecord 执行节点剩余并发槽（node_id 标签）。
func (m *Metrics) FreeSlotsRecord(ctx context.Context, nodeID string, v int64) {
	if m.FreeSlots != nil {
		m.FreeSlots.Record(ctx, v, metric.WithAttributes(attribute.String("node_id", nodeID)))
	}
}

// TerminalCount 终态计数（outcome 标签）。
func (m *Metrics) TerminalCount(ctx context.Context, outcome string) {
	if m.TasksTerminal != nil {
		m.TasksTerminal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
}

// RejectCount 注册拒绝计数（kind ∈ tombstone/attempt）。
func (m *Metrics) RejectCount(ctx context.Context, kind string) {
	if m.InprocessRejects != nil {
		m.InprocessRejects.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", kind)))
	}
}

// ResetCount 对账重置计数（reason 标签，核心健康信号）。
func (m *Metrics) ResetCount(ctx context.Context, reason string, n int64) {
	if n > 0 && m.ReconcileResets != nil {
		m.ReconcileResets.Add(ctx, n, metric.WithAttributes(attribute.String("reason", reason)))
	}
}

// HandlerDurationRecord handler 耗时（type 标签；调用方保证类型白名单）。
func (m *Metrics) HandlerDurationRecord(ctx context.Context, seconds float64, taskType string) {
	if m.HandlerDuration != nil {
		m.HandlerDuration.Record(ctx, seconds, metric.WithAttributes(attribute.String("type", taskType)))
	}
}
