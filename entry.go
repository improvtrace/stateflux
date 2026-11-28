// Package schedule 实现基于定时触发的任务调度器：cron 到点 → listener 分发
// 事件 → CronScheduler 取 factory 批次 → publisher 落表发布 → dispatch 按
// 集群节点表路由（queue:<resource>/<node>）→ queue 执行 → 对账收敛与重派。
// 本文件是组装与生命周期入口：Config/Run 与 wire 装配集（注入器见 wire.go）。
package schedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/wire"
	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
	"github.com/improvtrace/stateflux/tasks/persistents"
	"github.com/improvtrace/stateflux/tasks/sayhello"
)

// Config 是调度器的组装参数，经 wire（InitializeRuntime）流入各 provider。
// Repo 由调用方提供（states.Open + states.NewTaskPersistentRepo）。任务清单
// 不在配置里：由各任务包的 ProviderSet 经 NewSpecs 汇总（DESIGN.md §4 D12）。
type Config struct {
	RedisAddrs        []string
	RedisDB           int
	DMAddress         string // dm 服务的 unix socket 监听地址：dispatch 节点表与本节点 ID 来源
	DMServiceKey      string // dm 服务内部接口的服务密钥
	Repo              states.TaskPersistentRepo
	ReconcileInterval time.Duration // 对账间隔，0 → 30s
}

// Run 装配并运行调度器，阻塞直到 ctx 取消。组件构建经 wire 完成
// （InitializeRuntime），本函数只做生命周期编排。启动顺序：装配（先经 dm
// 确认本节点 ID）→ 首轮对账（先于一切发布：消化崩溃残留，把重启/停止前未
// 执行完的任务重新 dispatch 到任务队列）→ worker → 事件循环/定时/对账；
// 退出顺序：ctx 取消 → 各循环退出 → Shutdown 停 worker。
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		slog.Error("schedule: invalid runtime config", "error", err)
		return err
	}
	nodeID, err := dispatch.LocalNodeID(ctx, cfg.DMAddress, cfg.DMServiceKey)
	if err != nil {
		slog.Error("schedule: failed to resolve local node id", "dm_address", cfg.DMAddress, "error", err)
		return err
	}
	slog.Info("schedule: assembling runtime",
		"node", nodeID,
		"redis_addrs", len(cfg.RedisAddrs),
		"redis_db", cfg.RedisDB,
		"dm_address", cfg.DMAddress,
		"reconcile_interval", cfg.ReconcileInterval,
	)
	rt, err := InitializeRuntime(cfg, nodeID)
	if err != nil {
		return fmt.Errorf("schedule: assemble runtime: %w", err)
	}
	specs := rt.Registry.Specs()
	if len(specs) == 0 {
		return errors.New("schedule: task registry is empty")
	}
	// 后续一切调度只作用于启用的任务（TaskSpec.Enabled）：cron 条目、worker
	// 与恢复通道都以 Enabled() 为准；禁用任务的表行冻结在 pending。
	specs = rt.Registry.Enabled()
	if len(specs) == 0 {
		return errors.New("schedule: all tasks are disabled")
	}
	slog.Info("schedule: runtime assembled",
		"resources", len(specs),
		"timed_resources", len(rt.Registry.Timed()),
	)

	if err := rt.Server.RegisterTasks(handlers(ctx, rt.Repo, rt.Registry)); err != nil {
		return fmt.Errorf("schedule: register handlers: %w", err)
	}
	slog.Debug("schedule: task handlers registered", "resources", resourceNames(specs))

	// 首轮对账先于一切发布
	slog.Info("schedule: initial reconciliation started")
	rt.Reconciler.ReconcileOnce(ctx)
	slog.Info("schedule: initial reconciliation finished")

	if err := launchWorkers(ctx, rt.Server, rt.Registry, nodeID); err != nil {
		return err
	}

	rt.Listener.Register(rt.CronScheduler)
	slog.Debug("schedule: event handler registered", "handler", rt.CronScheduler.Name())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	start(&wg, func() error { return rt.Listener.Run(runCtx) })
	start(&wg, func() error { return rt.CronScheduler.Run(runCtx) })
	start(&wg, func() error { return rt.Reconciler.Run(runCtx) })
	slog.Info("schedule: scheduler running", "node", nodeID, "resources", len(specs))

	<-ctx.Done()
	slog.Info("schedule: shutdown initiated", "reason", ctx.Err())
	cancel()
	wg.Wait()
	slog.Debug("schedule: scheduler loops stopped")
	<-rt.Server.Shutdown().Done()
	slog.Info("schedule: shutdown complete")
	return ctx.Err()
}

// resourceNames 返回按字典序排列的资源名，供启动日志稳定输出。
func resourceNames(specs []*tasks.TaskSpec) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.Resource)
	}
	slices.Sort(names)
	return names
}

// validate 校验组装参数的必填项。
func (c Config) validate() error {
	if c.Repo == nil || len(c.RedisAddrs) == 0 || c.DMAddress == "" || c.DMServiceKey == "" {
		return errors.New("schedule: Repo, RedisAddrs, DMAddress and DMServiceKey are required")
	}
	return nil
}

// start 在 goroutine 中运行 fn，结束时标记 wg 完成。
func start(wg *sync.WaitGroup, fn func() error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = fn()
	}()
}

// launchWorkers 为每个资源在本节点的分节点队列（queue:<resource>/<node>）
// 启动一个消费者，并排空 worker 的错误通道。发布侧由 dispatch 按同一约定
// 路由，任务只会落入目标节点自己的队列。
func launchWorkers(ctx context.Context, server *queue.Server, reg *tasks.Registry, node string) error {
	specs := reg.Enabled() // 禁用任务不启动消费者（任务保留在注册表供 Get 查询）
	errCh := make(chan error, len(specs))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if err != nil {
					slog.Error("schedule: worker exited with error", "error", err)
				}
			}
		}
	}()
	for _, spec := range specs {
		concurrency := spec.Concurrency
		if concurrency <= 0 {
			concurrency = tasks.DefaultConcurrency
		}
		queueName := dispatch.QueueName(spec.Resource, node)
		worker := server.NewCustomQueueWorker(fmt.Sprintf("%s@%s", spec.Resource, node), concurrency, queueName)
		worker.LaunchAsync(errCh)
		slog.Info("schedule: worker launched", "resource", spec.Resource, "queue", queueName, "concurrency", concurrency)
	}
	return nil
}

// handlers 把注册的业务 handler 包装成带终态回写的版本：执行成功即回写
// succeeded；失败不内联回写——queue 可能仍在重试，失败终态由对账按
// backend 状态收敛。回写用脱离取消的 ctx，保证停机时不丢最后一次成功回写。
// 断言成立由 TaskSpec.Validate 的签名约定保证。
func handlers(ctx context.Context, repo states.TaskPersistentRepo, reg *tasks.Registry) map[string]any {
	wctx := context.WithoutCancel(ctx)
	specs := reg.Enabled() // 禁用任务不注册 handler（worker 也不启动，见 launchWorkers）
	out := make(map[string]any, len(specs))
	for _, ts := range specs {
		raw := ts.Handler.(func(string, int64) error)
		resource := ts.Resource
		out[ts.HandlerName()] = func(payload string, taskID int64) error {
			start := time.Now()
			slog.Debug("schedule: task execution started", "resource", resource, "task_id", taskID)
			err := raw(payload, taskID)
			elapsed := time.Since(start)
			if err != nil {
				slog.Warn("schedule: task execution failed",
					"resource", resource, "task_id", taskID,
					"duration_ms", elapsed.Milliseconds(), "error", err)
				return err
			}
			slog.Info("schedule: task execution succeeded",
				"resource", resource, "task_id", taskID,
				"duration_ms", elapsed.Milliseconds())
			if taskID != 0 {
				if werr := repo.MarkTerminal(wctx, taskID, states.TaskOutcomeSucceeded, ""); werr != nil {
					slog.Error("schedule: failed to mark task succeeded", "resource", resource, "task_id", taskID, "error", werr)
				} else {
					slog.Debug("schedule: task marked succeeded", "resource", resource, "task_id", taskID)
				}
			}
			return nil
		}
	}
	return out
}

// ProviderSet 是 stateflux 的完整 wire 装配集：任务清单（NewSpecs +
// 各任务自身的 ProviderSet，分散在各任务包内）与框架组件（redis/queue/
// dispatch/publisher/scheduler/reconciler）。接入自有业务任务时，在 NewSpecs
// 中追加任务定义及其 ProviderSet 即可。
var ProviderSet = wire.NewSet(
	persistents.ProviderSet,
	NewSpecs,
	tasks.NewRegistry,
	provideRepo,
	NewQueueConfig,
	NewRedisClient,
	NewQueueServer,
	provideLedger,
	provideDispatcher,
	provideListener,
	providePublisher,
	provideCronScheduler,
	provideReconciler,
	// 持久化转发通道：persistents.Forwarder 经 Publisher 接口重新发布内层
	// Signature，接口绑定到 dispatch.Dispatcher。
	wire.Bind(new(persistents.Publisher), new(*dispatch.Dispatcher)),
)

// Runtime 是 wire 装配出的调度器组件集，生命周期由 Run 编排。
type Runtime struct {
	Repo          states.TaskPersistentRepo
	Registry      *tasks.Registry
	Server        *queue.Server
	Listener      *Listener
	CronScheduler *CronScheduler
	Reconciler    *states.Reconciler
}

// NewSpecs 汇总任务清单——任务注册的直接维护点：新增任务 = 在 tasks/ 下
// 新建目录（无依赖的任务直接提供 Spec()，有依赖的经各自构造器注入），并在
// 此加 import 与一行。wire 无多绑定能力，清单是显式枚举。
// 当前注册：persistents 持久化转发的消费侧（不生产任务）与 sayhello 演示
// 任务（@every 30s 问候语，非 Durable 直发）。
func NewSpecs(forwarder *persistents.Forwarder) []*tasks.TaskSpec {
	return []*tasks.TaskSpec{
		persistents.Spec(forwarder),
		sayhello.Spec(),
	}
}

// provideRepo 从组装参数取任务表 repo。
func provideRepo(cfg Config) states.TaskPersistentRepo {
	return cfg.Repo
}

// defaultQueueResource 是 queue 框架兜底队列的资源名：本框架的发布都经
// dispatch 路由、始终带 RoutingKey，兜底队列只会被框架内部未路由的投递
// （如 queue 自带的周期接口）用到。这里不依赖任务清单也是 wire 装配的需要：
// Registry → NewSpecs → persistents.Forwarder → Dispatcher → Server → 本函数，
// 依赖 Registry 会成环。
const defaultQueueResource = "default"

// NewQueueConfig 构造 queue 配置，DefaultQueue 取本节点的兜底分节点队列。
func NewQueueConfig(nodeID string) *queue.Config {
	return &queue.Config{DefaultQueue: dispatch.QueueName(defaultQueueResource, nodeID)}
}

// NewRedisClient 构造共享 Redis 客户端（队列/水位/账本/定时锁共用）。
func NewRedisClient(cfg Config, qcnf *queue.Config) redis.UniversalClient {
	return queue.NewGoRedisClient(qcnf, cfg.RedisAddrs, cfg.RedisDB)
}

// defaultLockRetries 是分布式锁（定时任务防重）的加锁重试次数。
const defaultLockRetries = 3

// NewQueueServer 构造执行框架：broker/backend/lock 共享同一客户端。
func NewQueueServer(qcnf *queue.Config, rdb redis.UniversalClient) (*queue.Server, error) {
	return queue.NewServer(qcnf,
		queue.NewRedisBrokerWithClient(qcnf, rdb),
		queue.NewRedisBackendWithClient(qcnf, rdb),
		queue.NewRedisLockWithClient(rdb, defaultLockRetries),
	)
}

// provideLedger 构造记账（缺省 key）。
func provideLedger(rdb redis.UniversalClient) *states.Ledger {
	return states.NewLedger(rdb, "")
}

// provideDispatcher 构造发布器：节点表经 dm 服务监听地址（unix socket）查询。
func provideDispatcher(server *queue.Server, cfg Config) *dispatch.Dispatcher {
	return dispatch.New(server, cfg.DMAddress, cfg.DMServiceKey)
}

// provideListener 构造事件总线（缺省缓冲）。
func provideListener() *Listener {
	return NewListener(0)
}

// providePublisher 构造发布通道（定时批次与恢复通道共用）。
func providePublisher(repo states.TaskPersistentRepo, disp *dispatch.Dispatcher, reg *tasks.Registry, ledger *states.Ledger, nodeID string) *Publisher {
	return NewPublisher(repo, disp, reg, ledger, nodeID)
}

// provideCronScheduler 构造定时调度器。
func provideCronScheduler(reg *tasks.Registry, pub *Publisher, disp *dispatch.Dispatcher, listener *Listener, rdb redis.UniversalClient, nodeID string) (*CronScheduler, error) {
	return NewCronScheduler(reg, pub, disp, listener, rdb, nodeID)
}

// provideReconciler 构造对账器（backend 从 queue server 取）。
func provideReconciler(repo states.TaskPersistentRepo, server *queue.Server, ledger *states.Ledger, pub *Publisher, cfg Config) *states.Reconciler {
	return states.NewReconciler(repo, server.GetBackend(), ledger, pub, cfg.ReconcileInterval, 0)
}
