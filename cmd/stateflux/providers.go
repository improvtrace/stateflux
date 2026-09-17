package stateflux

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	coherencev1 "github.com/improvtrace/stateflux/api/stateflux/coherence/v1"
	dispatchv1 "github.com/improvtrace/stateflux/api/stateflux/dispatch/v1"
	forwardv1 "github.com/improvtrace/stateflux/api/stateflux/forward/v1"
	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	workerv1 "github.com/improvtrace/stateflux/api/stateflux/worker/v1"
	bizcoherence "github.com/improvtrace/stateflux/internal/biz/coherence"
	bizdispatch "github.com/improvtrace/stateflux/internal/biz/dispatch"
	"github.com/improvtrace/stateflux/internal/biz/forward"
	biztask "github.com/improvtrace/stateflux/internal/biz/task"
	bizworker "github.com/improvtrace/stateflux/internal/biz/worker"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/cacheview"
	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/eventbus"
	"github.com/improvtrace/stateflux/internal/eventbus/channel"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/mem"
	redischan "github.com/improvtrace/stateflux/internal/eventbus/channel/redis"
	"github.com/improvtrace/stateflux/internal/eventbus/channel/rpc"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/runtime/collector"
	"github.com/improvtrace/stateflux/internal/runtime/reconcile"
	"github.com/improvtrace/stateflux/internal/runtime/scheduler"
	"github.com/improvtrace/stateflux/internal/server"
	"github.com/improvtrace/stateflux/internal/task"
	"github.com/improvtrace/stateflux/internal/task/codec"
	"github.com/improvtrace/stateflux/internal/task/dispatch"
	"github.com/improvtrace/stateflux/internal/task/factory"
	"github.com/improvtrace/stateflux/internal/worker"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// 逻辑 channel 名（任务行 channel 列的取值域）：装配把名字映射到实现，换实现不改状态机。
const (
	// ChannelDefault 是默认异步队列（Redis list），对应 config.Dispatch.DefaultDelivery。
	ChannelDefault = "default"
	// ChannelSync 是同步 request/reply（unary RPC）。
	ChannelSync = "sync"
	// ChannelStream 是结果流/全双工（gRPC ResultStream）。
	ChannelStream = "stream"
)

// provideDialer 提供共享 gRPC 连接池。
func provideDialer() *rpc.Dialer { return rpc.NewDialer() }

// provideClusterView 按 config.Cluster 构造集群视图客户端（§15.1#3）。
func provideClusterView(cfg config.Config) (cluster.View, error) {
	return cluster.New(cfg.Cluster)
}

// provideNodeID 返回本节点唯一 ID：节点信息运行时从集群视图获取（§2.1），
// 取快照 Info.NodeID（集群服务端在 GetClusterInfo 响应中上报本节点身份）；
// 视图未上报（首次快照为空等场景）时退化为内置单节点身份。
func provideNodeID(cache *cluster.Cache) string {
	if id := cache.Snapshot().NodeID; id != "" {
		return id
	}
	return cluster.DefaultLocalNode.ID
}

// provideClusterCache 包装带快照的集群视图（热路径无网络 I/O）；
// 刷新间隔取集群 DSN 的 interval 参数（默认 3s，0 = 只按需拉取）。
func provideClusterCache(ctx context.Context, cfg config.Config, view cluster.View) (*cluster.Cache, func()) {
	opts, err := config.ParseClusterDSN(cfg.Cluster.DSN)
	if err != nil {
		opts = config.ClusterOptions{Interval: config.DefaultClusterInterval}
	}
	cache := cluster.NewCache(ctx, view, opts.Interval)
	return cache, boundedCleanup("cluster-cache", cfg.Server.ShutdownTimeout, func() { _ = cache.Close() })
}

// provideCacheView 构造任务异步执行视图（§15.1#9）：Redis 可用时走 Redis，否则退化为
// 进程内实现并在装配日志中显式声明。
func provideCacheView(cfg config.Config, d *data.Data) (cacheview.View, error) {
	opts := cacheview.Options{
		TaskTTL:   cfg.Dispatch.DedupeWindow,
		DedupeTTL: cfg.Dispatch.DedupeWindow,
		RouteTTL:  cfg.Coherence.RevisionTTL,
	}
	if d == nil || d.Redis() == nil {
		return cacheview.NewMem(opts), nil
	}
	return cacheview.NewRedis(d.Redis(), opts)
}

// provideEventBus 注册全部逻辑 channel（§3.2）：异步为 Redis 形态，同步为 unary RPC，
// 结果流为 gRPC stream；另提供内存 fake 以便单机冒烟与测试。
func provideEventBus(cfg config.Config, d *data.Data, dialer *rpc.Dialer) (*eventbus.EventBus, error) {
	if d == nil || d.Redis() == nil {
		return nil, errors.New("server: redis client is required for eventbus channels")
	}
	timeout := cfg.Dispatch.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ropts := redischan.Options{BlockTimeout: 2 * time.Second}

	list, err := redischan.NewList(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	zset, err := redischan.NewZSet(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	stream, err := redischan.NewStream(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}
	pubsub, err := redischan.NewPubSub(d.Redis(), ropts)
	if err != nil {
		return nil, err
	}

	channels := map[string]channel.Channel{
		ChannelDefault: list,
		ChannelSync:    rpc.NewUnary(dialer, timeout),
		ChannelStream:  rpc.NewStream(dialer, timeout),
		"redis-list":   list,
		"redis-zset":   zset,
		"redis-stream": stream,
		"redis-pubsub": pubsub,
		"rpc-unary":    rpc.NewUnary(dialer, timeout),
		"rpc-stream":   rpc.NewStream(dialer, timeout),
		"memory":       mem.NewMemory(),
	}
	return eventbus.NewEventBus(channels)
}

// describeChannel 便于启动日志输出装配结果。
func describeChannel(bus *eventbus.EventBus, name string) string {
	ch, err := bus.Resolve(name)
	if err != nil {
		return fmt.Sprintf("%s=unregistered", name)
	}
	return fmt.Sprintf("%s=%s", name, ch.Kind())
}

// ---- 领域与基建 ----

// provideMetrics 构造 OTel 指标集。
func provideMetrics() (*obs.Metrics, error) { return obs.New() }

// provideStore 构造任务账本组合根（§3.1）：回调派生复用节点雪花，保证 ID 全局唯一（§5.6）。
func provideStore(d *data.Data, ids *domain.Snowflake) repository.Store {
	return data.NewStoreWithIDGenerator(d, ids.Next)
}

// provideSnowflake 构造任务 ID 生成器（§3.1：客户端雪花）。
func provideSnowflake(nodeID string) *domain.Snowflake {
	return domain.NewSnowflake(nodeID)
}

// provideTaskRegistry 构造任务原型注册表并登记内置原型（§15.1#13）。
func provideTaskRegistry() (*task.Registry, error) {
	reg := task.NewRegistry()
	if err := biztask.RegisterPrototypes(reg); err != nil {
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
	if err := bizworker.RegisterCapabilities(reg, bizworker.DefaultVerifier{}, bizworker.DefaultUploader{}); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideHandlerRegistry 构造 handler 注册表并注册内置 handler。
func provideHandlerRegistry() (*worker.HandlerRegistry, error) {
	reg := worker.NewHandlerRegistry()
	if err := biztask.RegisterHandlers(reg); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideResultPublisher 构造结果发布器（worker.ResultSink）。
func provideResultPublisher(cfg config.Config, bus *eventbus.EventBus, cache *cluster.Cache, view cacheview.View) *biztask.ResultPublisher {
	channel := cfg.Dispatch.ResultChannel
	if channel == "" {
		channel = ChannelStream
	}
	return biztask.NewResultPublisher(biztask.ResultPublisherOptions{
		Bus:         bus,
		ChannelName: channel,
		Nodes:       cache,
		View:        view,
		Timeout:     cfg.Dispatch.Timeout,
	})
}

// provideWorkerRuntime 构造执行运行时（§5.4）。
func provideWorkerRuntime(cfg config.Config, nodeID string, bus *eventbus.EventBus, cdc *codec.Codec, handlers *worker.HandlerRegistry, publisher *biztask.ResultPublisher, view cacheview.View, metrics *obs.Metrics) *worker.Runtime {
	return worker.NewRuntime(worker.RuntimeOptions{
		WAL:            provideWAL(cfg),
		Bus:            bus,
		Codec:          cdc,
		Handlers:       handlers,
		Queues:         cfg.Runtime.WorkerQueues,
		QueueSource:    bizworker.NewQueueSource(view, cfg.Runtime.WorkerQueues),
		QueueRefresh:   cfg.Coherence.SyncInterval,
		NodeID:         nodeID,
		ResultSink:     publisher.Publish,
		Metrics:        metrics,
		ExecuteTimeout: cfg.Runtime.DispatchGrace,
	})
}

// provideWAL 构造结果 WAL（§5.4）：配置了目录走磁盘 append-only，否则进程内实现。
// 磁盘打开失败不阻塞启动：退化为内存 WAL，正确性仍由 R1 兜底（§6.2）。
func provideWAL(cfg config.Config) worker.WAL {
	if cfg.Runtime.WALDir == "" {
		return worker.NewWAL(cfg.Runtime.WALMaxEntries)
	}
	path := filepath.Join(cfg.Runtime.WALDir, "results.wal")
	disk, err := worker.OpenDiskWAL(path, cfg.Runtime.WALMaxEntries, cfg.Runtime.WALMaxBytes)
	if err != nil {
		return worker.NewWAL(cfg.Runtime.WALMaxEntries)
	}
	return disk
}

// ---- 转发 ----

// provideForwarder 构造节点间转发客户端（§15.1#6）。
func provideForwarder(cfg config.Config, nodeID string, dialer *rpc.Dialer, cache *cluster.Cache) *forward.Forwarder {
	return forward.NewForwarder(dialer, cache, nodeID, cfg.Dispatch.MaxHops)
}

// provideForwardRegistry 构造转发方法注册表（DispatchService 在 App 装配时登记）。
func provideForwardRegistry() *forward.Registry { return forward.NewRegistry() }

// provideForwardServer 构造转发服务端（§15.1#6）。
func provideForwardServer(cfg config.Config, nodeID string, reg *forward.Registry, fwd *forward.Forwarder) *forward.Server {
	return forward.NewServer(reg, fwd, nodeID, cfg.Dispatch.MaxHops)
}

// ---- 分发 ----

// provideDispatcher 构造调度侧分发器（§5.3）。
func provideDispatcher(cfg config.Config, nodeID string, bus *eventbus.EventBus, cache *cluster.Cache, view cacheview.View, cdc *codec.Codec, metrics *obs.Metrics) *dispatch.Dispatcher {
	return dispatch.New(dispatch.Options{
		Bus:     bus,
		Nodes:   cache,
		View:    view,
		Codec:   cdc,
		Self:    nodeID,
		Config:  cfg.Dispatch,
		Metrics: metrics,
	})
}

// provideDispatchServer 构造分发服务端（§15.1#5）。
func provideDispatchServer(cfg config.Config, nodeID string, d *dispatch.Dispatcher, cache *cluster.Cache, fwd *forward.Forwarder, metrics *obs.Metrics) *bizdispatch.DispatchServer {
	return bizdispatch.NewDispatchServer(bizdispatch.DispatchServerOptions{
		Dispatcher: d,
		Nodes:      cache,
		Forwarder:  fwd,
		Self:       nodeID,
		Config:     cfg.Dispatch,
		Metrics:    metrics,
	})
}

// ---- 共识 ----

// provideCoherenceStore 构造共识快照持有者（§15.1#4）。
func provideCoherenceStore() *bizcoherence.CoherenceStore { return bizcoherence.NewCoherenceStore() }

// provideCoherenceServer 构造共识服务端（§15.1#4）。
func provideCoherenceServer(store *bizcoherence.CoherenceStore, view cacheview.View, metrics *obs.Metrics) *bizcoherence.CoherenceServer {
	return bizcoherence.NewCoherenceServer(store, view, metrics)
}

// provideCoherenceSyncer 构造调度侧共识同步器：分配全部 WorkerQueues。
func provideCoherenceSyncer(cfg config.Config, nodeID string, store *bizcoherence.CoherenceStore, cache *cluster.Cache, dialer *rpc.Dialer, metrics *obs.Metrics) *bizcoherence.CoherenceSyncer {
	pusher := bizcoherence.NewCoherencePusher(dialer, cache, cfg.Dispatch.Timeout)
	return bizcoherence.NewCoherenceSyncer(bizcoherence.CoherenceSyncerOptions{
		Store:     store,
		Allocator: bizcoherence.Allocator{SchedulerNodeID: nodeID},
		Pusher:    pusher,
		Nodes:     cache,
		Queues:    cfg.Runtime.WorkerQueues,
		Interval:  cfg.Coherence.SyncInterval,
		Metrics:   metrics,
	})
}

// provideCoherencePuller 构造执行侧共识拉取器（§15.1#4）。
func provideCoherencePuller(cfg config.Config, nodeID string, store *bizcoherence.CoherenceStore, cache *cluster.Cache, dialer *rpc.Dialer, view cacheview.View, metrics *obs.Metrics) *bizcoherence.CoherencePuller {
	return bizcoherence.NewCoherencePuller(bizcoherence.CoherencePullerOptions{
		Store:    store,
		View:     view,
		Dialer:   dialer,
		Nodes:    cache,
		Self:     nodeID,
		Interval: cfg.Coherence.SyncInterval,
		Timeout:  cfg.Dispatch.Timeout,
		Metrics:  metrics,
	})
}

// ---- 归集与对账 ----

// provideCollector 构造结果归集器（§5.5）。
func provideCollector(store repository.Store, metrics *obs.Metrics) *collector.Collector {
	return collector.New(collector.Options{Ledger: store, Metrics: metrics})
}

// provideCollectorRunner 构造 Redis 结果通道订阅器（默认禁用，结果走 gRPC stream）。
func provideCollectorRunner(cfg config.Config, bus *eventbus.EventBus, c *collector.Collector, metrics *obs.Metrics) *collector.Runner {
	channel := cfg.Dispatch.ResultChannel
	if channel == ChannelStream {
		// 默认结果路径由 biztask.ExecutorServer.ResultStream 直接归集，无需订阅。
		channel = ""
	}
	return collector.NewRunner(collector.RunnerOptions{Bus: bus, ChannelName: channel, Collector: c, Metrics: metrics})
}

// provideReconciler 构造 R1/R2 对账器（§6.2）。
func provideReconciler(cfg config.Config, store repository.Store, view cacheview.View, probe reconcile.ChannelProbe, metrics *obs.Metrics) *reconcile.Reconciler {
	return reconcile.New(reconcile.Options{
		Ledger:      store,
		Orphans:     store,
		Probe:       probe,
		View:        view,
		Interval:    cfg.Runtime.ReconcileInterval,
		Grace:       cfg.Runtime.DispatchGrace,
		Skew:        0, // skew 待定（§14.11），默认 0
		Limit:       cfg.Runtime.ClaimBatch,
		PurgePrefix: "",
		Metrics:     metrics,
	})
}

// redisChannelProbe 用 Redis PING 报告通道可用性（R4，§14.9）。
type redisChannelProbe struct{ client redis.UniversalClient }

// ChannelsAvailable 实现 reconcile.ChannelProbe。
func (p redisChannelProbe) ChannelsAvailable(ctx context.Context) (bool, error) {
	if p.client == nil {
		return true, nil
	}
	if err := p.client.Ping(ctx).Err(); err != nil {
		return false, err
	}
	return true, nil
}

// provideChannelProbe 构造 R4 探测（Redis 不可用时返回 nil，R4 关闭）。
func provideChannelProbe(d *data.Data) reconcile.ChannelProbe {
	if d == nil || d.Redis() == nil {
		return nil
	}
	return redisChannelProbe{client: d.Redis()}
}

// ---- 调度 ----

// provideCycle 构造默认调度周期（§5.2/§5.3）。
func provideCycle(cfg config.Config, nodeID string, store repository.Store, d *dispatch.Dispatcher, bus *eventbus.EventBus, metrics *obs.Metrics) *scheduler.LedgerCycle {
	return scheduler.NewCycle(scheduler.CycleOptions{
		Ledger:       store,
		Dispatcher:   d,
		Bus:          bus,
		NodeID:       nodeID,
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
	return n, boundedCleanup("pg-notify", cfg.Server.ShutdownTimeout, func() { _ = n.Close() })
}

// provideSchedulerGroup 按配置构造多个调度实例（不同触发方式，§15.1#11）。
func provideSchedulerGroup(cfg config.Config, cycle *scheduler.LedgerCycle, store *bizcoherence.CoherenceStore, notifier *data.TaskNotifier, metrics *obs.Metrics) *scheduler.Group {
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
	if err := biztask.RegisterFactories(reg, cfg.Runtime.FactoryInterval, ChannelDefault); err != nil {
		return nil, err
	}
	return reg, nil
}

// provideFactoryManager 构造工厂运行器。
func provideFactoryManager(reg *factory.Registry, e *biztask.Enqueuer) *factory.Manager {
	return factory.NewManager(reg, e.Sink())
}

// provideEnqueuer 构造入队器（§5.1）。
func provideEnqueuer(store repository.Store, ids *domain.Snowflake, cfg config.Config) *biztask.Enqueuer {
	return biztask.NewEnqueuer(store, ids, ChannelDefault)
}

// ---- 服务端 ----

// provideExecutorServer 构造 ExecutorService 服务端（§7）。
func provideExecutorServer(rt *worker.Runtime, c *collector.Collector, metrics *obs.Metrics) *biztask.ExecutorServer {
	return biztask.NewExecutorServer(rt, c, metrics)
}

// provideCapabilityServer 构造 CapabilityService 服务端（§15.1#7）。
func provideCapabilityServer(reg *worker.Registry, metrics *obs.Metrics) *bizworker.CapabilityServer {
	return bizworker.NewCapabilityServer(reg, metrics)
}

// provideGRPCServer 注册全部契约服务（§7、§15.1）。
func provideGRPCServer(
	executor *biztask.ExecutorServer,
	capability *bizworker.CapabilityServer,
	dispatchSrv *bizdispatch.DispatchServer,
	coherence *bizcoherence.CoherenceServer,
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

// Start 实现 server.Component：在后台 goroutine 运行。
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

// Stop 实现 server.Component。
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

// provideComponents 汇总全部运行时组件（顺序即启动顺序）。执行侧（worker、coherence 拉取）
// 在所有节点运行；控制面（调度/归集/对账/工厂/共识同步）经 electionGate 仅在当选的调度
// 节点运行（§2.1）。
func provideComponents(
	cfg config.Config,
	nodeID string,
	cache *cluster.Cache,
	bus *eventbus.EventBus,
	writer workerComponent,
	schedulers schedulerComponent,
	reconciler reconcileComponent,
	syncer syncerComponent,
	puller pullerComponent,
	factories factoryComponent,
	resultRunner collectorRunnerComponent,
) []server.Component {
	components := make([]server.Component, 0, 9)
	// eventbus 最先启动、最后停止：待全部组件停稳后再关闭结果流等底层通道，
	// 服务端 GracefulStop 才能立即排空长连接，而不是等满关机预算。
	if bus != nil {
		components = append(components, eventBusComponent{bus: bus})
	}
	if writer.v != nil {
		components = append(components, writer.v)
	}
	if puller.v != nil {
		components = append(components, puller.v)
	}
	// 各控制面组件按节点服务权限 gate（外置集群角色推导）：
	// 归集 → NodePermissionCollect；调度 / 工厂 / 共识同步 / 对账 → NodePermissionSchedule。
	gate := func(name string, permission cluster.NodePermission, inner server.Component) server.Component {
		if inner == nil {
			return nil
		}
		return server.NewElectionGate(name, cache, nodeID, permission, inner, server.DefaultElectionPollInterval, nil)
	}
	if resultRunner.v != nil {
		components = append(components, gate("collector", cluster.NodePermissionCollect, resultRunner.v))
	}
	if syncer.v != nil {
		components = append(components, gate("coherence", cluster.NodePermissionSchedule, syncer.v))
	}
	if factories.v != nil {
		components = append(components, gate("factory", cluster.NodePermissionSchedule, factories.v))
	}
	if schedulers.v != nil {
		components = append(components, gate("scheduler", cluster.NodePermissionSchedule, newRunnerComponent("schedulers", schedulers.v.Run)))
	}
	if reconciler.v != nil {
		components = append(components, gate("reconcile", cluster.NodePermissionSchedule, reconciler.v))
	}
	return components
}

// eventBusComponent 把 eventbus 的生命周期纳入组件链：Start 无操作，Stop 关闭全部
// channel 底层连接（结果流客户端等）。
type eventBusComponent struct{ bus *eventbus.EventBus }

// Start 实现 server.Component：eventbus 在装配期已就绪。
func (eventBusComponent) Start(context.Context) error { return nil }

// Stop 实现 server.Component：关闭全部 channel（幂等）。
func (c eventBusComponent) Stop() error {
	if c.bus == nil {
		return nil
	}
	return c.bus.Close()
}

// 以下具名包装类型用于让 wire 区分多个 server.Component 依赖。
type (
	workerComponent          struct{ v server.Component }
	schedulerComponent       struct{ v *scheduler.Group }
	reconcileComponent       struct{ v server.Component }
	syncerComponent          struct{ v server.Component }
	pullerComponent          struct{ v server.Component }
	factoryComponent         struct{ v server.Component }
	collectorRunnerComponent struct{ v server.Component }
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

func provideSyncerComponent(s *bizcoherence.CoherenceSyncer) syncerComponent {
	return syncerComponent{v: s}
}

func providePullerComponent(p *bizcoherence.CoherencePuller) pullerComponent {
	return pullerComponent{v: p}
}

func provideFactoryComponent(m *factory.Manager) factoryComponent {
	return factoryComponent{v: newRunnerComponent("factories", func(ctx context.Context) error { m.Run(ctx); return nil })}
}

func provideCollectorRunnerComponent(r *collector.Runner) collectorRunnerComponent {
	return collectorRunnerComponent{v: r}
}

// dataOpen 包装 data.Open 以便 wire 统一收集清理函数（§15.1#1）。
func dataOpen(ctx context.Context, cfg config.Config) (*data.Data, func(), error) {
	d, cleanup, err := data.Open(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return d, boundedCleanup("data", cfg.Server.ShutdownTimeout, cleanup), nil
}

// provideApp 构造 App 并完成跨引用登记（转发方法注册，§15.1#6）。
func provideApp(
	cfg config.Config,
	grpcServer *grpc.Server,
	httpServer *http.Server,
	components []server.Component,
	forwardRegistry *forward.Registry,
	dispatchServer *bizdispatch.DispatchServer,
	health *server.Health,
) (*server.App, error) {
	if forwardRegistry != nil && dispatchServer != nil {
		if err := forwardRegistry.Register(bizdispatch.DispatchMethod, dispatchServer.HandleForwarded); err != nil {
			return nil, err
		}
	}
	return server.NewApp(server.AppOptions{
		Config:     cfg,
		GRPCServer: grpcServer,
		HTTPServer: httpServer,
		Components: components,
		Health:     health,
	}), nil
}

// NewServeDeps 按 stateflux 的 serve 子命令需要创建依赖 provider 实例：
// 返回值经 NewServeCommand 注入子命令，命令层不感知装配细节。
func NewServeDeps() ServeDeps {
	return ServeDeps{AppFactory: newApplication}
}
