// cmd/stateflux 是官方服务装配（参考实现，§1.2.8）：全角色合一的单进程入口。
// 框架本体在各角色包（api/scheduler/executor/collector/factory/reconcile/store/queue/cluster），
// 业务方可整体嵌入或将部分角色（如仅 executor）嵌入自身进程自行装配。
//
// 角色启用（§2.1）：API 与 Executor 所有实例常驻；Scheduler + Collector + Reconciler +
// Factory 仅在 ClusterView 指向本实例（外部选举）时运行；static 模式下全部角色赋给本进程。
package main

import (
	"context"
	"crypto/sha1"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	apiv1 "github.com/improvtrace/stateflux/proto/gen/apiv1"
	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"

	"github.com/improvtrace/stateflux/api"
	"github.com/improvtrace/stateflux/cluster"
	"github.com/improvtrace/stateflux/collector"
	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/executor"
	"github.com/improvtrace/stateflux/factory"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/reconcile"
	"github.com/improvtrace/stateflux/scheduler"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "stateflux:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		pgDSN        = flag.String("pg-dsn", envOr("STATEFLUX_PG_DSN", ""), "PostgreSQL DSN（任务表组）")
		redisAddr    = flag.String("redis-addr", envOr("STATEFLUX_REDIS_ADDR", "127.0.0.1:6379"), "Redis 地址")
		nodeID       = flag.String("node-id", envOr("STATEFLUX_NODE_ID", defaultNodeID()), "节点唯一 ID")
		advertise    = flag.String("advertise", envOr("STATEFLUX_ADVERTISE", "127.0.0.1:7001"), "本节点 gRPC 对外地址（集群可见）")
		clusterMode  = flag.String("cluster-mode", envOr("STATEFLUX_CLUSTER_MODE", config.ClusterModeStatic), "集群模式：static | external")
		clusterEp    = flag.String("cluster-endpoint", envOr("STATEFLUX_CLUSTER_ENDPOINT", ""), "外部选举服务 gRPC 地址（external 模式）")
		apiListen    = flag.String("api-listen", ":7000", "业务接入 gRPC 监听地址")
		intlListen   = flag.String("internal-listen", ":7001", "内部 gRPC（Execute/Collect）监听地址")
		walDir       = flag.String("wal-dir", "", "执行侧结果 WAL 目录（默认按节点临时目录）")
		demo         = flag.Bool("demo", false, "注册演示 Handler（demo.echo / demo.fail）用于冒烟")
		rebuildStart = flag.Bool("rebuild-on-start", false, "启动时执行 R4 全量重建（Redis 丢失恢复）")
		debug        = flag.Bool("debug", false, "调试日志")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: levelOf(*debug)}))
	if *pgDSN == "" {
		return errors.New("-pg-dsn is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Config{}
	cfg.ApplyDefaults()
	cfg.Cluster.NodeID = *nodeID
	cfg.Cluster.Address = *advertise
	cfg.Cluster.Mode = *clusterMode
	cfg.Internal.Listen = *intlListen
	cfg.API.Listen = *apiListen
	cfg.Reconcile.RebuildOnStart = *rebuildStart
	cfg.Log.Debug = *debug

	// 观测装配（§6.5）：全局 MeterProvider；未启用 OTel 时 no-op。
	mp, err := config.NewMeterProvider(ctx, cfg.OTel)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		_ = mp.Close(shutdownCtx)
	}()
	metrics, err := obs.New()
	if err != nil {
		return err
	}

	// 权威存储：ent + PG 四阶段表（含迁移 + 幂等键局部唯一索引 + NOTIFY 触发器）。
	sf, err := sdk.NewSnowflake(snowflakeWorker(*nodeID))
	if err != nil {
		return err
	}
	st, err := store.Open(store.Options{DSN: *pgDSN, Snowflake: sf, Logger: log})
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	// Redis 视图：队列 + inprocess 集合（§3.2）。
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()
	q := queue.New(rdb)

	// 集群视图：static 单机闭环 / external 外部选举（§2.1）。
	var view cluster.View
	switch *clusterMode {
	case config.ClusterModeExternal:
		if *clusterEp == "" {
			return errors.New("-cluster-endpoint is required in external mode")
		}
		view = cluster.NewRPCClient(*clusterEp, *nodeID, *advertise, nil, cfg.Cluster.RefreshInterval, true)
	default:
		view = cluster.NewStatic(cluster.StaticConfig{NodeID: *nodeID, Address: *advertise})
	}
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	defer refreshCancel()
	go refreshLoop(refreshCtx, view, log)

	// 执行角色：Handler 注册表 + 消费循环 + 结果 WAL + gRPC server（所有节点常驻）。
	registry := sdk.NewRegistry()
	registerHandlers(registry, *demo, log)
	dir := *walDir
	if dir == "" {
		dir = fmt.Sprintf("%s/stateflux-wal-%s", os.TempDir(), *nodeID)
	}
	wal, err := executor.OpenWAL(executor.WALConfig{
		Dir:          dir,
		MaxEntries:   cfg.Executor.WAL.MaxEntries,
		MaxBytes:     cfg.Executor.WAL.MaxBytes,
		SegmentBytes: cfg.Executor.WAL.SegmentBytes,
	}, log)
	if err != nil {
		return err
	}
	defer wal.Close()
	exec, err := executor.New(executor.Options{
		NodeID:      *nodeID,
		Registry:    registry,
		Queue:       q,
		WAL:         wal,
		Concurrency: cfg.Executor.Concurrency,
		LeaseTTL:    cfg.Executor.LeaseTTL, LeaseRenewInterval: cfg.Executor.LeaseRenewInterval,
		BRPOPTimeout: cfg.Executor.BRPOPTimeout, CapacityReportInterval: cfg.Executor.CapacityReportInterval,
		Metrics: metrics, Logger: log,
	})
	if err != nil {
		return err
	}

	pool := executor.NewPool()
	defer pool.Close()

	// 内部 gRPC（Execute/Collect/Ack）。
	intlSrv := grpc.NewServer()
	dispatchv1.RegisterExecutorServiceServer(intlSrv, executor.NewServer(exec, log))
	intlLis, err := net.Listen("tcp", cfg.Internal.Listen)
	if err != nil {
		return fmt.Errorf("listen internal: %w", err)
	}
	go func() {
		<-ctx.Done()
		stopped := make(chan struct{})
		go func() {
			intlSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			intlSrv.Stop()
		}
	}()
	log.Info("internal grpc listening", "addr", cfg.Internal.Listen)
	go func() {
		if err := intlSrv.Serve(intlLis); err != nil {
			log.Error("internal grpc serve failed", "err", err)
			stop()
		}
	}()

	// API 角色：业务接入点（所有节点常驻）。
	apiSvc := api.New(st, cfg.API, log)
	apiSrv := grpc.NewServer()
	apiv1.RegisterTaskServiceServer(apiSrv, apiSvc)
	apiv1.RegisterAdminServiceServer(apiSrv, apiSvc.Admin())
	apiLis, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen api: %w", err)
	}
	go func() {
		<-ctx.Done()
		apiSrv.GracefulStop()
	}()
	log.Info("api grpc listening", "addr", cfg.API.Listen)
	go func() {
		if err := apiSrv.Serve(apiLis); err != nil {
			log.Error("api grpc serve failed", "err", err)
			stop()
		}
	}()

	// 执行角色消费循环。
	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		if err := exec.Run(ctx); err != nil {
			log.Error("executor run failed", "err", err)
		}
	}()

	// 调度侧角色（Scheduler/Collector/Reconciler/Factory）：仅当外部选举指向本实例时运行。
	leaderDone := leadLoop(ctx, view, st, q, pool, cfg, metrics, log)

	log.Info("stateflux started",
		"node_id", *nodeID, "mode", *clusterMode,
		"scheduler", view.SchedulerID())
	<-ctx.Done()
	log.Info("stateflux shutting down")
	<-leaderDone
	select {
	case <-execDone:
	case <-time.After(30 * time.Second):
		log.Warn("executor shutdown timeout")
	}
	log.Info("stateflux stopped")
	return nil
}

// leadLoop 监督调度侧角色的启停（故障切换无内部交接协议：认领与对账全部幂等，§2.1）。
func leadLoop(
	ctx context.Context, view cluster.View, st store.Store, q *queue.Queue,
	pool *executor.Pool, cfg config.Config, metrics *obs.Metrics, log *slog.Logger,
) <-chan struct{} {
	done := make(chan struct{})
	var (
		mu      sync.Mutex
		cancel  context.CancelFunc
		running bool
	)
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			self := view.Self()
			isLeader := view.SchedulerID() == self.ID && self.ID != ""
			mu.Lock()
			switch {
			case isLeader && !running:
				running = true
				var leadCtx context.Context
				leadCtx, cancel = context.WithCancel(ctx)
				startLeaderRoles(leadCtx, view, st, q, pool, cfg, metrics, log)
				log.Info("scheduler role started (elected)")
			case !isLeader && running:
				running = false
				if cancel != nil {
					cancel()
				}
				log.Info("scheduler role stopped (leadership lost)")
			}
			mu.Unlock()
			select {
			case <-ctx.Done():
				mu.Lock()
				if running && cancel != nil {
					cancel()
				}
				mu.Unlock()
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

// startLeaderRoles 装配并启动调度侧全部角色（§2.1：Scheduler + Collector + Reconciler + Factory）。
func startLeaderRoles(
	ctx context.Context, view cluster.View, st store.Store, q *queue.Queue,
	pool *executor.Pool, cfg config.Config, metrics *obs.Metrics, log *slog.Logger,
) {
	// NOTIFY 唤醒扇出（低延迟优化；tick 兜底不可关，§5.1）。
	wake := make(chan struct{}, 8)
	watcher := store.NewNotifyWatcher(dsnForListen(cfg.Postgres.DSN))
	go watcher.Run(ctx)
	go func() {
		for range watcher.C() {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()

	// 晋升器（约束晋升 pending → schedulable，§5.2）。
	promoter := scheduler.NewPromoter(scheduler.PromoterOptions{
		Store: st, Interval: cfg.Scheduler.PromoterInterval, Batch: cfg.Scheduler.PromoteBatch,
		TypeConcurrency: cfg.Scheduler.TypeConcurrency, Metrics: metrics, Logger: log,
	})
	go promoter.Run(ctx, wake)

	// 归集器（先建，作为调度器的同步结果出口；§5.5 统一 flush 通道）。
	coll := collector.New(collector.Options{
		Store: st, Queue: q, View: view, Pool: pool,
		Cfg: cfg.Collector, Metrics: metrics, Logger: log,
	})
	go coll.Run(ctx)

	// 调度器（约束晋升后的自适应认领 + 分发，§5.3）。
	sched, err := scheduler.New(scheduler.Options{
		NodeID: view.Self().ID, Store: st, Queue: q, View: view, Pool: pool,
		Sink: coll, Cfg: cfg.Scheduler, Metrics: metrics, Logger: log,
	})
	if err != nil {
		log.Error("scheduler init failed", "err", err)
		return
	}
	go sched.Run(ctx, wake)

	// 对账（R1~R4，§6.3）。
	rec := reconcile.New(reconcile.Options{
		Store: st, Queue: q, Cfg: cfg.Reconcile, Metrics: metrics, Logger: log,
	})
	go rec.Run(ctx)

	// TaskFactory（period/cron 周期任务，§5.7；条目由嵌入方/配置注入）。
	if len(cfg.Factory.Entries) > 0 {
		f, err := factory.New(st, cfg.Factory, metrics, log)
		if err != nil {
			log.Error("factory init failed", "err", err)
		} else {
			go f.Run(ctx)
		}
	}
}

// refreshLoop 周期刷新集群视图（external 模式为 RPC 拉取；static 为 no-op）。
func refreshLoop(ctx context.Context, view cluster.View, log *slog.Logger) {
	interval := time.Second
	if r, ok := view.(interface{ Interval() time.Duration }); ok {
		interval = r.Interval()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := view.Refresh(ctx); err != nil {
			log.Debug("cluster view refresh failed", "err", err)
		}
	}
}

// registerHandlers Handler 注册点（§7）：嵌入方在此注册业务执行器；demo 模式提供冒烟示例。
func registerHandlers(reg *sdk.Registry, demo bool, log *slog.Logger) {
	if !demo {
		return
	}
	reg.Register(&sdk.HandlerFunc{
		HandlerType: "demo.echo",
		Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
			return task.Payload, nil
		},
	})
	reg.Register(&sdk.HandlerFunc{
		HandlerType: "demo.fail",
		Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
			return nil, errors.New("demo failure")
		},
	})
	reg.Register(&sdk.HandlerFunc{
		HandlerType: "demo.slow",
		Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return []byte(`{"slow":true}`), nil
			}
		},
	})
	log.Info("demo handlers registered", "types", reg.Types())
}

// ---- 小工具 ----

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func defaultNodeID() string {
	h, err := os.Hostname()
	if err != nil {
		return "node-0"
	}
	return h
}

// snowflakeWorker 节点 ID 哈希到 10 位 worker 空间（同 ID 集群内稳定）。
func snowflakeWorker(nodeID string) int64 {
	sum := sha1.Sum([]byte(nodeID))
	return int64(sum[0])<<2 | int64(sum[1]>>6) // 0..1023
}

func levelOf(debug bool) slog.Level {
	if debug {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// dsnForListen NOTIFY 专用连接 DSN（与连接池隔离，§5.1 实施约束）。
func dsnForListen(dsn string) string { return dsn }
