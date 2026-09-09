package config

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

// MeterProviderHandle 持有装配好的 OTel MeterProvider，Close flush 后释放资源。
type MeterProviderHandle struct {
	MeterProvider *metric.MeterProvider
	shutdown      func(context.Context) error
}

// Close flush 并释放 Provider 资源。
func (h *MeterProviderHandle) Close(ctx context.Context) error {
	if h == nil || h.shutdown == nil {
		return nil
	}
	return h.shutdown(ctx)
}

// NewMeterProvider 按 §6.5 装配全局 MeterProvider（周期性 Reader + OTLP Exporter，
// 默认 60s 推送，endpoint/协议可配），Resource 携带 service.name、node_id、版本/环境标签。
// 指标是框架的单一采集标准（§6.5）；链路追踪的传播头由 TaskMessage.trace_headers 承载（§3.3），
// 全局 TracerProvider 留给业务方/部署侧注入，不在框架内强制采样导出。
// 未启用时保持默认（noop）Provider，零开销。
func NewMeterProvider(ctx context.Context, cfg OTelConfig) (*MeterProviderHandle, error) {
	cfg.applyDefaults()
	if !cfg.Enabled || cfg.Endpoint == "" {
		return &MeterProviderHandle{}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.Version),
			semconv.DeploymentEnvironmentName(cfg.Env),
			semconv.ServiceInstanceID(cfg.NodeID),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("config: build otel resource: %w", err)
	}

	var exp metric.Exporter
	switch cfg.Exporter {
	case "grpc":
		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
			otlpmetricgrpc.WithHeaders(cfg.Headers),
		}
		if cfg.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err = otlpmetricgrpc.New(ctx, opts...)
	case "http":
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(cfg.Endpoint),
			otlpmetrichttp.WithHeaders(cfg.Headers),
		}
		if cfg.Insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err = otlpmetrichttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("config: unknown otel exporter %q (grpc|http)", cfg.Exporter)
	}
	if err != nil {
		return nil, fmt.Errorf("config: new otlp metric exporter: %w", err)
	}

	reader := metric.NewPeriodicReader(exp, metric.WithInterval(cfg.ExportInterval))
	mp := metric.NewMeterProvider(metric.WithResource(res), metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	return &MeterProviderHandle{
		MeterProvider: mp,
		shutdown: func(ctx context.Context) error {
			return mp.Shutdown(ctx)
		},
	}, nil
}

// ShutdownTimeout Provider 关闭默认超时。
const ShutdownTimeout = 5 * time.Second
