//go:build wireinject
// +build wireinject

// 本文件是 wire 注入声明（§15.1#1）：项目启动依赖 wire 生成装配代码。
// 运行 `make wire`（等价 `wire ./internal/server`）生成 wire_gen.go。
//
//go:generate go tool github.com/google/wire/cmd/wire
package server

import (
	"context"

	"github.com/google/wire"

	"github.com/improvtrace/stateflux/internal/config"
)

// ProviderSet 是全部装配提供者（§15.1#1）。
var ProviderSet = wire.NewSet(
	// 配置与基建
	provideMetrics,
	provideDialer,
	provideClusterView,
	provideClusterCache,
	provideEventBus,
	provideCacheView,
	dataOpen,

	// 领域 / 任务运行时
	provideStore,
	provideSnowflake,
	provideTaskRegistry,
	provideCodec,
	provideEnqueuer,
	provideFactoryRegistry,
	provideFactoryManager,
	provideFactoryComponent,

	// 执行侧
	provideCapabilityRegistry,
	provideHandlerRegistry,
	provideResultPublisher,
	provideWorkerRuntime,
	provideWorkerComponent,

	// 转发 / 分发
	provideForwarder,
	provideForwardRegistry,
	provideForwardServer,
	provideDispatcher,
	provideDispatchServer,

	// 共识
	provideCoherenceStore,
	provideCoherenceServer,
	provideCoherenceSyncer,
	provideCoherencePuller,
	provideSyncerComponent,
	providePullerComponent,

	// 归集 / 对账
	provideCollector,
	provideCollectorRunner,
	provideCollectorRunnerComponent,
	provideReconciler,
	provideReconcileComponent,
	provideChannelProbe,

	// 调度
	provideCycle,
	provideSchedulerGroup,
	provideSchedulerComponent,
	provideTaskNotifier,

	// 服务端与组件
	provideExecutorServer,
	provideCapabilityServer,
	provideGRPCServer,
	provideComponents,
	NewHealth,
	NewHTTPServer,
	provideApp,
)

// InitializeApplication 是 wire 生成的装配入口（§15.1#1）：cmd/stateflux 只调用它。
func InitializeApplication(ctx context.Context, cfg config.Config) (*App, func(), error) {
	panic(wire.Build(ProviderSet))
}
