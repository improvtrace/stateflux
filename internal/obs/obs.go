// Package obs 集中定义 §6.4 最小指标集（单一采集标准 = OpenTelemetry）。各角色包只依赖
// 注入的 *obs.Metrics，不感知导出协议；未启用 OTel 时 instruments 为 no-op，零开销。
//
// 标签纪律（§6.4）：只允许低基数标签（channel 名、duplex、kind、outcome、reason、scope、
// type 白名单）；task_id / idempotency_key 一律禁止入标签。通信面指标（eventbus.*）只描述
// 通道行为，不作为任务正确性来源——正确性信号来自 PG（对账重置、名额与控制 fence 拒绝），
// 因为通道本身被设计为可丢失、可重复（§1.2.1、§9.3）。
//
// 水位型指标（wal.backlog、concurrency.reservations）采用同步 Int64Gauge——采集点位收敛在既有
// 写路径，在测量点直接记录，语义与 ObservableGauge 相同且免去回调装配（§6.4）。
package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics stateflux 全框架指标集（§6.4 最小指标集）。
type Metrics struct {
	meter metric.Meter

	// stateflux.eventbus.send —— 通信面：发送尝试数（channel / duplex 标签）。
	EventBusSend metric.Int64Counter
	// stateflux.eventbus.subscribe —— 通信面：订阅建立数（channel / duplex 标签）。
	EventBusSubscribe metric.Int64Counter
	// stateflux.eventbus.errors —— 通信面：发送、订阅与投递错误数（channel / kind 标签）。
	EventBusErrors metric.Int64Counter
	// stateflux.collector.results —— Collector：归集事件数（outcome 标签）。
	CollectorResults metric.Int64Counter
	// stateflux.reconcile.resets —— 对账：R1/R3/R5 重置次数（reason 标签，核心健康信号）。
	ReconcileResets metric.Int64Counter
	// stateflux.control.fence_rejects —— 控制面/归集：陈旧 epoch 或 attempt 的拒绝数（kind 标签）。
	ControlFenceRejects metric.Int64Counter
	// stateflux.wal.backlog.entries/.bytes —— worker：WAL 未确认条数与字节数（反压水位）。
	WALBacklogEntries metric.Int64Gauge
	WALBacklogBytes   metric.Int64Gauge
	// stateflux.handler.duration —— worker：handler 耗时（type 标签）。
	HandlerDuration metric.Float64Histogram
	// stateflux.concurrency.reservations —— 调度：在途并发名额（scope 标签）。
	ConcurrencyReservations metric.Int64Gauge
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
			m.ControlFenceRejects, err = mm.Int64Counter("stateflux.control.fence_rejects")
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
			m.ConcurrencyReservations, err = mm.Int64Gauge("stateflux.concurrency.reservations")
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

// ---- 录制辅助（低基数标签在此收敛） ----

// SendRecord 通道发送尝试（channel 名 + 双工形态；best-effort，不代表送达）。
func (m *Metrics) SendRecord(ctx context.Context, channelName, duplex string) {
	if m.EventBusSend != nil {
		m.EventBusSend.Add(ctx, 1, metric.WithAttributes(
			attribute.String("channel", channelName),
			attribute.String("duplex", duplex),
		))
	}
}

// SubscribeRecord 通道订阅建立。
func (m *Metrics) SubscribeRecord(ctx context.Context, channelName, duplex string) {
	if m.EventBusSubscribe != nil {
		m.EventBusSubscribe.Add(ctx, 1, metric.WithAttributes(
			attribute.String("channel", channelName),
			attribute.String("duplex", duplex),
		))
	}
}

// ErrorRecord 通道错误（kind ∈ send/subscribe/deliver/connect）。
func (m *Metrics) ErrorRecord(ctx context.Context, channelName, kind string) {
	if m.EventBusErrors != nil {
		m.EventBusErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("channel", channelName),
			attribute.String("kind", kind),
		))
	}
}

// CollectorResultRecord 归集事件数（outcome ∈ succeeded/failed/dead/stale）。
func (m *Metrics) CollectorResultRecord(ctx context.Context, outcome string, n int64) {
	if n > 0 && m.CollectorResults != nil {
		m.CollectorResults.Add(ctx, n, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
}

// ResetCount 对账重置计数（reason 标签，核心健康信号）。
func (m *Metrics) ResetCount(ctx context.Context, reason string, n int64) {
	if n > 0 && m.ReconcileResets != nil {
		m.ReconcileResets.Add(ctx, n, metric.WithAttributes(attribute.String("reason", reason)))
	}
}

// FenceRejectCount 陈旧 epoch / attempt 拒绝计数（kind ∈ control_epoch/attempt）。
func (m *Metrics) FenceRejectCount(ctx context.Context, kind string, n int64) {
	if n > 0 && m.ControlFenceRejects != nil {
		m.ControlFenceRejects.Add(ctx, n, metric.WithAttributes(attribute.String("kind", kind)))
	}
}

// WALBacklogRecord WAL 未确认条数/字节数。
func (m *Metrics) WALBacklogRecord(ctx context.Context, entries, bytes int64) {
	if m.WALBacklogEntries != nil {
		m.WALBacklogEntries.Record(ctx, entries)
	}
	if m.WALBacklogBytes != nil {
		m.WALBacklogBytes.Record(ctx, bytes)
	}
}

// HandlerDurationRecord handler 耗时（type 标签；调用方保证类型白名单）。
func (m *Metrics) HandlerDurationRecord(ctx context.Context, seconds float64, taskType string) {
	if m.HandlerDuration != nil {
		m.HandlerDuration.Record(ctx, seconds, metric.WithAttributes(attribute.String("type", taskType)))
	}
}

// ReservationsRecord 在途并发名额（scope 标签；由 PG 权威值投影，不是独立账本）。
func (m *Metrics) ReservationsRecord(ctx context.Context, scope string, v int64) {
	if m.ConcurrencyReservations != nil {
		m.ConcurrencyReservations.Record(ctx, v, metric.WithAttributes(attribute.String("scope", scope)))
	}
}
