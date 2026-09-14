package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	dispatchv1 "github.com/improvtrace/stateflux/api/dispatch/v1"
	coherencev1 "github.com/improvtrace/stateflux/api/stateflux/coherence/v1"
	forwardv1 "github.com/improvtrace/stateflux/api/stateflux/forward/v1"
	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	workerv1 "github.com/improvtrace/stateflux/api/stateflux/worker/v1"
	"github.com/improvtrace/stateflux/internal/biz"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
	"github.com/improvtrace/stateflux/internal/forward"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/runtime/collector"
	"github.com/improvtrace/stateflux/internal/runtime/reconcile"
	"github.com/improvtrace/stateflux/internal/runtime/scheduler"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/codec"
	"github.com/improvtrace/stateflux/internal/task/dispatch"
	"github.com/improvtrace/stateflux/internal/task/factory"
	"github.com/improvtrace/stateflux/internal/worker"
)

// ---- 领域与基建 ----

// provideMetrics 构造 OTel 指标集。
func provideMetrics() (*obs.Metrics, error) { return obs.New() }

// provideStore 构造任务账本组合根（§3.1）：回调派生复用节点雪花，保证 ID 全局唯一（§5.6）。
func provideStore(d *data.Data, ids *domain.Snowflake) repository.Store {
	return data.NewStoreWithIDGenerator(d, ids.Next)
}

// provideSnowflake 构造任务 ID 生成器（§3.1：客户端雪花）。
func provideSnowflake(cfg config.Config) *domain.Snowflake {
	return domain.NewSnowflake(cfg.Node.NodeID)
}

// provideTaskRegistry 构造任务原型注册表并登记内置原型（§15.1#13）。
func provideTaskRegistry() (*task.Registry, error) {
	reg := task.NewRegistry()
	if err := biz.RegisterPrototypes(reg); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideCodec 构造签名化 codec（machinery 风格，§15.1#13）。
func provideCodec(reg *task.Registry) *codec.Codec { return codec.New(reg) }

// ---- 执行侧 ----

// provideCapabilityRegistry 构造能力注册表并注册内置能力（§15.1#7/#8）。
func provideCapabilityRegistry() (*worker.Registry, error) {
	reg := worker.NewRegistry()
	if err := biz.RegisterCapabilities(reg, biz.DefaultVerifier{}, biz.DefaultUploader{}); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideHandlerRegistry 构造 handler 注册表并注册内置 handler。
func provideHandlerRegistry() (*worker.HandlerRegistry, error) {
	reg := worker.NewHandlerRegistry()
	if err := biz.RegisterHandlers(reg); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideResultPublisher 构造结果发布器（worker.ResultSink）。
func provideResultPublisher(cfg config.Config, bus *eventbus.EventBus, cache *cluster.Cache, view cacheview.View) *biz.ResultPublisher {
	return biz.NewResultPublisher(biz.ResultPublisherOptions{
		Bus:         bus,
		ChannelName: ChannelStream,
		Nodes:       cache,
		View:        view,
		Timeout:     cfg.Dispatch.Timeout,
	})
}

// provideWorkerRuntime 构造执行运行时（§5.4）。
func provideWorkerRuntime(cfg config.Config, bus *eventbus.EventBus, cdc *codec.Codec, handlers *worker.HandlerRegistry, publisher *biz.ResultPublisher, metrics *obs.Metrics) *worker.Runtime {
	return worker.NewRuntime(worker.RuntimeOptions{
		Bus:            bus,
		Codec:          cdc,
		Handlers:       handlers,
		Queues:         cfg.Runtime.WorkerQueues,
		NodeID:         cfg.Node.NodeID,
		ResultSink:     publisher.Publish,
		Metrics:        metrics,
		ExecuteTimeout: cfg.Runtime.DispatchGrace,
	})
}

// ---- 转发 ----

// provideForwarder 构造节点间转发客户端（§15.1#6）。
func provideForwarder(cfg config.Config, dialer *rpc.Dialer, cache *cluster.Cache) *forward.Forwarder {
	return forward.NewForwarder(dialer, cache, cfg.Node.NodeID, cfg.Dispatch.MaxHops)
}

// provideForwardRegistry 构造转发方法注册表（DispatchService 在 App 装配时登记）。
func provideForwardRegistry() *forward.Registry { return forward.NewRegistry() }

// provideForwardServer 构造转发服务端（§15.1#6）。
func provideForwardServer(cfg config.Config, reg *forward.Registry, fwd *forward.Forwarder) *forward.Server {
	return forward.NewServer(reg, fwd, cfg.Node.NodeID, cfg.Dispatch.MaxHops)
}

// ---- 分发 ----

// provideDispatcher 构造调度侧分发器（§5.3）。
func provideDispatcher(cfg config.Config, bus *eventbus.EventBus, cache *cluster.Cache, view cacheview.View, cdc *codec.Codec, metrics *obs.Metrics) *dispatch.Dispatcher {
	return dispatch.New(dispatch.Options{
		Bus:     bus,
		Nodes:   cache,
		View:    view,
		Codec:   cdc,
		Self:    cfg.Node.NodeID,
		Config:  cfg.Dispatch,
		Metrics: metrics,
	})
}

// provideDispatchServer 构造分发服务端（§15.1#5）。
func provideDispatchServer(cfg config.Config, d *dispatch.Dispatcher, cache *cluster.Cache, fwd *forward.Forwarder, metrics *obs.Metrics) *biz.DispatchServer {
	return biz.NewDispatchServer(biz.DispatchServerOptions{
		Dispatcher: d,
		Nodes:      cache,
		Forwarder:  fwd,
		Self:       cfg.Node.NodeID,
		Config:     cfg.Dispatch,
		Metrics:    metrics,
	})
}

// ---- 共识 ----

// provideCoherenceStore 构造共识快照持有者（§15.1#4）。
func provideCoherenceStore() *biz.CoherenceStore { return biz.NewCoherenceStore() }

// provideCoherenceServer 构造共识服务端（§15.1#4）。
func provideCoherenceServer(store *biz.CoherenceStore, view cacheview.View, metrics *obs.Metrics) *biz.CoherenceServer {
	return biz.NewCoherenceServer(store, view, metrics)
}

// provideCoherenceSyncer 构造调度侧共识同步器：分配全部 WorkerQueues。
func provideCoherenceSyncer(cfg config.Config, store *biz.CoherenceStore, cache *cluster.Cache, dialer *rpc.Dialer, metrics *obs.Metrics) *biz.CoherenceSyncer {
	pusher := biz.NewCoherencePusher(dialer, cache, cfg.Cluster.Timeout)
	return biz.NewCoherenceSyncer(biz.CoherenceSyncerOptions{
		Store:     store,
		Allocator: biz.Allocator{SchedulerNodeID: cfg.Node.NodeID},
		Pusher:    pusher,
		Nodes:     cache,
		Queues:    cfg.Runtime.WorkerQueues,
		Interval:  cfg.Coherence.SyncInterval,
		Metrics:   metrics,
	})
}

// provideCoherencePuller 构造执行侧共识拉取器（§15.1#4）。
func provideCoherencePuller(cfg config.Config, store *biz.CoherenceStore, cache *cluster.Cache, dialer *rpc.Dialer, view cacheview.View, metrics *obs.Metrics) *biz.CoherencePuller {
	return biz.NewCoherencePuller(biz.CoherencePullerOptions{
		Store:    store,
		View:     view,
		Dialer:   dialer,
		Nodes:    cache,
		Self:     cfg.Node.NodeID,
		Interval: cfg.Coherence.SyncInterval,
		Timeout:  cfg.Cluster.Timeout,
		Metrics:  metrics,
	})
}

// ---- 归集与对账 ----

// provideCollector 构造结果归集器（§5.5）。
func provideCollector(store repository.Store, metrics *obs.Metrics) *collector.Collector {
	return collector.New(collector.Options{Ledger: store, Metrics: metrics})
}

// provideCollectorRunner 构造 Redis 结果通道订阅器（默认禁用，结果走 gRPC stream）。
func provideCollectorRunner(bus *eventbus.EventBus, c *collector.Collector, metrics *obs.Metrics) *collector.Runner {
	return collector.NewRunner(collector.RunnerOptions{Bus: bus, ChannelName: "", Collector: c, Metrics: metrics})
}

// provideReconciler 构造 R1/R2 对账器（§6.2）。
func provideReconciler(cfg config.Config, store repository.Store, view cacheview.View, metrics *obs.Metrics) *reconcile.Reconciler {
	return reconcile.New(reconcile.Options{
		Ledger:      store,
		Orphans:     store,
		View:        view,
		Interval:    cfg.Runtime.ReconcileInterval,
		Grace:       cfg.Runtime.DispatchGrace,
		Skew:        0, // skew 待定（§14.11），默认 0
		Limit:       cfg.Runtime.ClaimBatch,
		PurgePrefix: "",
		Metrics:     metrics,
	})
}

// ---- 调度 ----

// provideCycle 构造默认调度周期（§5.2/§5.3）。
func provideCycle(cfg config.Config, store repository.Store, d *dispatch.Dispatcher, bus *eventbus.EventBus, metrics *obs.Metrics) *scheduler.LedgerCycle {
	return scheduler.NewCycle(scheduler.CycleOptions{
		Ledger:       store,
		Dispatcher:   d,
		Bus:          bus,
		NodeID:       cfg.Node.NodeID,
		PromoteLimit: cfg.Runtime.ClaimBatch,
		ClaimBatch:   cfg.Runtime.ClaimBatch,
		Config:       cfg.Dispatch,
		Metrics:      metrics,
	})
}

// provideTaskNotifier 建立 PG LISTEN/NOTIFY 唤醒（§5.1）：通知只是优化，建立失败不阻塞启动。
func provideTaskNotifier(cfg config.Config) (*data.TaskNotifier, func()) {
	n, err := data.NewTaskNotifier(cfg.PG.DSN, cfg.PG.SearchPath)
	if err != nil {
		return nil, func() {}
	}
	return n, func() { _ = n.Close() }
}

// provideSchedulerGroup 按配置构造多个调度实例（不同触发方式，§15.1#11）。
func provideSchedulerGroup(cfg config.Config, cycle *scheduler.LedgerCycle, store *biz.CoherenceStore, notifier *data.TaskNotifier, metrics *obs.Metrics) *scheduler.Group {
	names := cfg.Runtime.SchedulerTriggers
	if len(names) == 0 {
		names = []string{"tick"}
	}
	group := make([]*scheduler.Scheduler, 0, len(names))
	for _, name := range names {
		var trigger scheduler.Trigger
		switch name {
		case "notify":
			fallback := cfg.Runtime.TickInterval * 10
			if fallback <= 0 {
				fallback = time.Second
			}
			var wake <-chan struct{}
			if notifier != nil {
				wake = notifier.Notifications()
			}
			trigger = scheduler.NotifyTrigger{Notifications: wake, FallbackInterval: fallback}
		case "coherence":
			trigger = scheduler.CoherenceTrigger{Source: store, PollInterval: cfg.Coherence.SyncInterval}
		case "manual":
			trigger = scheduler.ManualTrigger{C: make(chan struct{})}
		default:
			trigger = scheduler.TickTrigger{Interval: cfg.Runtime.TickInterval}
		}
		group = append(group, scheduler.New(scheduler.Options{
			Name:    name,
			Trigger: scheduler.NamedTrigger{InstanceName: name, Inner: trigger},
			Cycle:   cycle,
			Metrics: metrics,
		}))
	}
	return scheduler.NewGroup(group...)
}

// ---- 工厂 ----

// provideFactoryRegistry 构造工厂注册表并注册内置工厂（§15.1#12）。
func provideFactoryRegistry(cfg config.Config) (*factory.Registry, error) {
	reg := factory.NewRegistry()
	if err := biz.RegisterFactories(reg, cfg.Runtime.FactoryInterval, ChannelDefault); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideFactoryManager 构造工厂运行器。
func provideFactoryManager(reg *factory.Registry, e *biz.Enqueuer) *factory.Manager {
	return factory.NewManager(reg, e.Sink())
}

// provideEnqueuer 构造入队器（§5.1）。
func provideEnqueuer(store repository.Store, ids *domain.Snowflake, cfg config.Config) *biz.Enqueuer {
	return biz.NewEnqueuer(store, ids, ChannelDefault)
}

// ---- 服务端 ----

// provideExecutorServer 构造 ExecutorService 服务端（§7）。
func provideExecutorServer(rt *worker.Runtime, c *collector.Collector, metrics *obs.Metrics) *biz.ExecutorServer {
	return biz.NewExecutorServer(rt, c, metrics)
}

// provideCapabilityServer 构造 CapabilityService 服务端（§15.1#7）。
func provideCapabilityServer(reg *worker.Registry, metrics *obs.Metrics) *biz.CapabilityServer {
	return biz.NewCapabilityServer(reg, metrics)
}

// provideGRPCServer 注册全部契约服务（§7、§15.1）。
func provideGRPCServer(
	executor *biz.ExecutorServer,
	capability *biz.CapabilityServer,
	dispatchSrv *biz.DispatchServer,
	coherence *biz.CoherenceServer,
	forwardSrv *forward.Server,
) *grpc.Server {
	s := grpc.NewServer()
	taskv1.RegisterExecutorServiceServer(s, executor)
	workerv1.RegisterCapabilityServiceServer(s, capability)
	dispatchv1.RegisterDispatchServiceServer(s, dispatchSrv)
	coherencev1.RegisterCoherenceServiceServer(s, coherence)
	forwardv1.RegisterForwardServiceServer(s, forwardSrv)
	return s
}

// ---- 组件生命周期包装 ----

// runnerComponent 把阻塞式 Run(ctx) 运行器包装为可启停组件。
type runnerComponent struct {
	name string
	run  func(ctx context.Context) error

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newRunnerComponent(name string, run func(ctx context.Context) error) *runnerComponent {
	return &runnerComponent{name: name, run: run}
}

// Start 实现 Component：在后台 goroutine 运行。
func (c *runnerComponent) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		_ = c.run(runCtx)
	}()
	return nil
}

// Stop 实现 Component。
func (c *runnerComponent) Stop() error {
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.cancel = nil
	c.done = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

// provideComponents 汇总全部运行时组件（顺序即启动顺序）。
func provideComponents(
	writer workerComponent,
	schedulers schedulerComponent,
	reconciler reconcileComponent,
	syncer syncerComponent,
	puller pullerComponent,
	factories factoryComponent,
	resultRunner collectorRunnerComponent,
) []Component {
	components := make([]Component, 0, 8)
	if writer.v != nil {
		components = append(components, writer.v)
	}
	if resultRunner.v != nil {
		components = append(components, resultRunner.v)
	}
	if syncer.v != nil {
		components = append(components, syncer.v)
	}
	if puller.v != nil {
		components = append(components, puller.v)
	}
	if factories.v != nil {
		components = append(components, factories.v)
	}
	if schedulers.v != nil {
		components = append(components, newRunnerComponent("schedulers", schedulers.v.Run))
	}
	if reconciler.v != nil {
		components = append(components, reconciler.v)
	}
	return components
}

// 以下具名包装类型用于让 wire 区分多个 Component 依赖。
type (
	workerComponent          struct{ v Component }
	schedulerComponent       struct{ v *scheduler.Group }
	reconcileComponent       struct{ v Component }
	syncerComponent          struct{ v Component }
	pullerComponent          struct{ v Component }
	factoryComponent         struct{ v Component }
	collectorRunnerComponent struct{ v Component }
)

func provideWorkerComponent(rt *worker.Runtime) workerComponent {
	return workerComponent{v: rt}
}

func provideSchedulerComponent(g *scheduler.Group) schedulerComponent {
	return schedulerComponent{v: g}
}

func provideReconcileComponent(r *reconcile.Reconciler) reconcileComponent {
	return reconcileComponent{v: r}
}

func provideSyncerComponent(s *biz.CoherenceSyncer) syncerComponent { return syncerComponent{v: s} }

func providePullerComponent(p *biz.CoherencePuller) pullerComponent { return pullerComponent{v: p} }

func provideFactoryComponent(m *factory.Manager) factoryComponent {
	return factoryComponent{v: newRunnerComponent("factories", func(ctx context.Context) error { m.Run(ctx); return nil })}
}

func provideCollectorRunnerComponent(r *collector.Runner) collectorRunnerComponent {
	return collectorRunnerComponent{v: r}
}

// dataOpen 包装 data.Open 以便 wire 统一收集清理函数（§15.1#1）。
func dataOpen(ctx context.Context, cfg config.Config) (*data.Data, func(), error) {
	return data.Open(ctx, cfg)
}

// provideApp 构造 App 并完成跨引用登记（转发方法注册，§15.1#6）。
func provideApp(
	cfg config.Config,
	grpcServer *grpc.Server,
	httpServer *http.Server,
	components []Component,
	forwardRegistry *forward.Registry,
	dispatchServer *biz.DispatchServer,
) (*App, error) {
	if forwardRegistry != nil && dispatchServer != nil {
		if err := forwardRegistry.Register(biz.DispatchMethod, dispatchServer.HandleForwarded); err != nil {
			return nil, err
		}
	}
	return NewApp(AppOptions{
		Config:     cfg,
		GRPCServer: grpcServer,
		HTTPServer: httpServer,
		Components: components,
	}), nil
}
