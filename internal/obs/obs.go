// Package obs 集中定义 §6.4 最小指标集 + §15.1#10 的分发/转发/共识/能力指标
// （单一采集标准 = OpenTelemetry）。各角色包只依赖注入的 *obs.Metrics，不感知导出协议；
// 未启用 OTel 时 instruments 为 no-op，零开销。
//
// 标签纪律（§6.4）：只允许低基数标签（channel 名、duplex、kind、outcome、reason、type、
// semantics、delivery、direction、factory、capability 白名单）；task_id / idempotency_key
// 一律禁止入标签。通信面指标只描述通道行为，不作为任务正确性来源——正确性信号来自 PG。
//
// 空值语义：全部录制方法在接收者为 nil 或 instrument 为 nil 时静默返回，调用方无需判空。
package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics stateflux 全框架指标集（§6.4 + §15.1#10）。
type Metrics struct {
	meter metric.Meter

	// ---- 通信面（§6.4） ----
	EventBusSend      metric.Int64Counter
	EventBusSubscribe metric.Int64Counter
	EventBusErrors    metric.Int64Counter

	// ---- 归集与对账（§6.4） ----
	CollectorResults metric.Int64Counter
	ReconcileResets  metric.Int64Counter

	// ---- worker 水位（§6.4） ----
	WALBacklogEntries metric.Int64Gauge
	WALBacklogBytes   metric.Int64Gauge
	HandlerDuration   metric.Float64Histogram

	// ---- 分发（§15.1#5） ----
	// stateflux.dispatch.total —— 分发尝试数（semantics / delivery / result 标签）。
	DispatchTotal metric.Int64Counter
	// stateflux.dispatch.forwarded —— 节点间转发的分发数（direction 标签）。
	DispatchForwarded metric.Int64Counter

	// ---- 转发（§15.1#6） ----
	// stateflux.forward.total —— 转发处理数（direction = sent/received/relayed / result）。
	ForwardTotal metric.Int64Counter

	// ---- 共识信息（§15.1#4） ----
	// stateflux.coherence.sync —— 共识同步次数（result = ok/not_modified/error）。
	CoherenceSync metric.Int64Counter
	// stateflux.coherence.revision —— 当前共识 revision。
	CoherenceRevision metric.Int64Gauge

	// ---- 能力（§15.1#7） ----
	// stateflux.capability.invoke —— 能力调用数（capability / result 标签）。
	CapabilityInvoke metric.Int64Counter

	// ---- 工厂（§15.1#12） ----
	// stateflux.factory.generated —— 工厂生成任务数（factory 标签）。
	FactoryGenerated metric.Int64Counter
}

// New 创建指标集（otel.Meter 经 config 装配的全局 MeterProvider 解析；未启用 OTel 时为 no-op）。
func New() (*Metrics, error) {
	meter := otel.Meter("stateflux")
	m := &Metrics{meter: meter}
	instruments := []struct {
		mk func(metric.Meter) error
	}{
		{func(mm metric.Meter) (err error) {
			m.EventBusSend, err = mm.Int64Counter("stateflux.eventbus.send")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.EventBusSubscribe, err = mm.Int64Counter("stateflux.eventbus.subscribe")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.EventBusErrors, err = mm.Int64Counter("stateflux.eventbus.errors")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.CollectorResults, err = mm.Int64Counter("stateflux.collector.results")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.ReconcileResets, err = mm.Int64Counter("stateflux.reconcile.resets")
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
			m.HandlerDuration, err = mm.Float64Histogram("stateflux.handler.duration", metric.WithUnit("s"))
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.DispatchTotal, err = mm.Int64Counter("stateflux.dispatch.total")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.DispatchForwarded, err = mm.Int64Counter("stateflux.dispatch.forwarded")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.ForwardTotal, err = mm.Int64Counter("stateflux.forward.total")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.CoherenceSync, err = mm.Int64Counter("stateflux.coherence.sync")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.CoherenceRevision, err = mm.Int64Gauge("stateflux.coherence.revision")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.CapabilityInvoke, err = mm.Int64Counter("stateflux.capability.invoke")
			return
		}},
		{func(mm metric.Meter) (err error) {
			m.FactoryGenerated, err = mm.Int64Counter("stateflux.factory.generated")
			return
		}},
	}
	for _, ins := range instruments {
		if err := ins.mk(meter); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// ---- 录制辅助（低基数标签在此收敛；nil 安全） ----

// SendRecord 通道发送尝试。
func (m *Metrics) SendRecord(ctx context.Context, channelName, duplex string) {
	if m == nil || m.EventBusSend == nil {
		return
	}
	m.EventBusSend.Add(ctx, 1, metric.WithAttributes(
		attribute.String("channel", channelName),
		attribute.String("duplex", duplex),
	))
}

// SubscribeRecord 通道订阅建立。
func (m *Metrics) SubscribeRecord(ctx context.Context, channelName, duplex string) {
	if m == nil || m.EventBusSubscribe == nil {
		return
	}
	m.EventBusSubscribe.Add(ctx, 1, metric.WithAttributes(
		attribute.String("channel", channelName),
		attribute.String("duplex", duplex),
	))
}

// ErrorRecord 通道错误（kind ∈ send/subscribe/deliver/connect/cacheview）。
func (m *Metrics) ErrorRecord(ctx context.Context, channelName, kind string) {
	if m == nil || m.EventBusErrors == nil {
		return
	}
	m.EventBusErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("channel", channelName),
		attribute.String("kind", kind),
	))
}

// CollectorResultRecord 归集事件数（outcome ∈ succeeded/failed/dead/stale）。
func (m *Metrics) CollectorResultRecord(ctx context.Context, outcome string, n int64) {
	if m == nil || m.CollectorResults == nil || n <= 0 {
		return
	}
	m.CollectorResults.Add(ctx, n, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// ResetCount 对账重置计数（reason 标签，核心健康信号）。
func (m *Metrics) ResetCount(ctx context.Context, reason string, n int64) {
	if m == nil || m.ReconcileResets == nil || n <= 0 {
		return
	}
	m.ReconcileResets.Add(ctx, n, metric.WithAttributes(attribute.String("reason", reason)))
}

// WALBacklogRecord WAL 未确认条数/字节数。
func (m *Metrics) WALBacklogRecord(ctx context.Context, entries, bytes int64) {
	if m == nil {
		return
	}
	if m.WALBacklogEntries != nil {
		m.WALBacklogEntries.Record(ctx, entries)
	}
	if m.WALBacklogBytes != nil {
		m.WALBacklogBytes.Record(ctx, bytes)
	}
}

// HandlerDurationRecord handler 耗时（type 标签；调用方保证类型白名单）。
func (m *Metrics) HandlerDurationRecord(ctx context.Context, seconds float64, taskType string) {
	if m == nil || m.HandlerDuration == nil {
		return
	}
	m.HandlerDuration.Record(ctx, seconds, metric.WithAttributes(attribute.String("type", taskType)))
}

// DispatchRecord 分发尝试（semantics / delivery / result）。
func (m *Metrics) DispatchRecord(ctx context.Context, semantics, delivery, result string, n int64) {
	if m == nil || m.DispatchTotal == nil || n <= 0 {
		return
	}
	m.DispatchTotal.Add(ctx, n, metric.WithAttributes(
		attribute.String("semantics", semantics),
		attribute.String("delivery", delivery),
		attribute.String("result", result),
	))
}

// DispatchForwardedRecord 节点间转发的分发数（direction = sent/received）。
func (m *Metrics) DispatchForwardedRecord(ctx context.Context, direction string, n int64) {
	if m == nil || m.DispatchForwarded == nil || n <= 0 {
		return
	}
	m.DispatchForwarded.Add(ctx, n, metric.WithAttributes(attribute.String("direction", direction)))
}

// ForwardRecord 转发处理（direction = sent/received/relayed，result = ok/error）。
func (m *Metrics) ForwardRecord(ctx context.Context, direction, result string, n int64) {
	if m == nil || m.ForwardTotal == nil || n <= 0 {
		return
	}
	m.ForwardTotal.Add(ctx, n, metric.WithAttributes(
		attribute.String("direction", direction),
		attribute.String("result", result),
	))
}

// CoherenceSyncRecord 共识同步（result = ok/not_modified/error）。
func (m *Metrics) CoherenceSyncRecord(ctx context.Context, result string, n int64) {
	if m == nil || m.CoherenceSync == nil || n <= 0 {
		return
	}
	m.CoherenceSync.Add(ctx, n, metric.WithAttributes(attribute.String("result", result)))
}

// CoherenceRevisionRecord 当前共识 revision。
func (m *Metrics) CoherenceRevisionRecord(ctx context.Context, revision int64) {
	if m == nil || m.CoherenceRevision == nil {
		return
	}
	m.CoherenceRevision.Record(ctx, revision)
}

// CapabilityInvokeRecord 能力调用（capability 名白名单，result = ok/error）。
func (m *Metrics) CapabilityInvokeRecord(ctx context.Context, capability, result string, n int64) {
	if m == nil || m.CapabilityInvoke == nil || n <= 0 {
		return
	}
	m.CapabilityInvoke.Add(ctx, n, metric.WithAttributes(
		attribute.String("capability", capability),
		attribute.String("result", result),
	))
}

// FactoryGeneratedRecord 工厂生成任务数。
func (m *Metrics) FactoryGeneratedRecord(ctx context.Context, factory string, n int64) {
	if m == nil || m.FactoryGenerated == nil || n <= 0 {
		return
	}
	m.FactoryGenerated.Add(ctx, n, metric.WithAttributes(attribute.String("factory", factory)))
}
