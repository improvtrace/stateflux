// Package stateflux 定义 stateflux 的 cobra 命令：serve 子命令完成 flag/env/配置
// 文件 → config → obs 装配 → wire 注入 → 启动的完整生命周期（§8、§15.1#1）；
// 收到 SIGINT/SIGTERM 后优雅退出，再次收到信号强制退出。进程入口在 cmd/main.go。
package stateflux

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/server"
)

// ServeDeps 是 serve 子命令的依赖注入面：cmd 装配层根据子命令需要创建依赖
// provider 实例，经 NewServeCommand 注入；cmd/stateflux 不感知具体装配。
type ServeDeps struct {
	// AppFactory 按 serve 解析出的配置装配应用并返回资源释放函数
	// （cmd 装配层的 wire 注入实现）。
	AppFactory func(ctx context.Context, cfg config.Config) (*server.App, func(), error)
}

// NewServeCommand returns the serve subcommand. Configuration precedence:
// defaults < config file < environment variables < command-line flags.
func NewServeCommand(deps ServeDeps) *cobra.Command {
	var configFile string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start a stateflux server node (gRPC + HTTP + control-plane runtimes)",
		Long: "Start a stateflux server node hosting the gRPC services, the " +
			"health/diagnostics HTTP server and the control-plane runtimes " +
			"(scheduler, collector, reconcile, factory, coherence sync) gated " +
			"by cluster election.",
		Example: `  # Start with a config file
  stateflux serve --config stateflux.yaml

  # Override single settings via flags
  stateflux serve --config stateflux.yaml --cluster-dsn local:// --grpc-addr 127.0.0.1:9090`,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := config.NewViper(configFile)
			if err != nil {
				return err
			}
			config.BindFlags(v, cmd.Flags())
			cfg, err := config.FromViper(v)
			if err != nil {
				return err
			}
			return run(deps, cfg)
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configFile, "config", "", "path to the config file (yaml/toml/json/etc.; empty = defaults, env and flags only)")
	fs.String("pg-dsn", "", "PostgreSQL data source name")
	fs.String("redis-addrs", "", "comma-separated Redis address list")
	fs.String("cluster-dsn", "", "cluster view DSN: grpc://host:port, http://host[:port][/path] or local:// (single node); params via URL query (timeout, poll_interval, node_id, address)")
	fs.String("grpc-addr", "", "gRPC listen address")
	fs.String("http-addr", "", "HTTP listen address")
	return cmd
}

// run 执行一次完整生命周期并返回错误。所有资源释放都在本函数内显式完成：
// 不使用 log.Fatal/os.Exit 提前返回，避免跳过 DB 关闭与 OTel 数据刷出。
func run(deps ServeDeps, cfg config.Config) error {
	ctx, stopSignals := shutdownContext()
	defer stopSignals()

	shutdownObs, err := obs.Setup(ctx, cfg.Obs)
	if err != nil {
		return fmt.Errorf("obs setup: %w", err)
	}

	// 启动依赖注入的 AppFactory（§15.1#1）：全部构造与依赖关系由 cmd 的 wire_gen.go 决定。
	app, cleanup, err := deps.AppFactory(ctx, cfg)
	if err != nil {
		flushObs(shutdownObs, cfg)
		return fmt.Errorf("wire init: %w", err)
	}

	startedAt := time.Now()
	log.Printf("stateflux: [config] grpc=%s http=%s cluster=%s triggers=%v queues=%v shutdown_timeout=%s",
		cfg.Server.GRPCAddr, cfg.Server.HTTPAddr,
		cfg.Cluster.DSN, cfg.Runtime.SchedulerTriggers, cfg.Runtime.WorkerQueues,
		cfg.Server.ShutdownTimeout)
	log.Printf("stateflux: [start] launching gRPC/HTTP services and components")

	// Run 在信号到达时返回：内部先停组件与服务面，随后这里按装配顺序逆序释放资源。
	runErr := app.Run(ctx)

	log.Printf("stateflux: [shutdown] runtime stopped (uptime=%s, err=%v)",
		time.Since(startedAt).Round(time.Millisecond), runErr)
	cleanupResources(cleanup, cfg)
	flushObs(shutdownObs, cfg)

	if runErr != nil {
		return fmt.Errorf("run: %w", runErr)
	}
	log.Printf("stateflux: [exit] stopped cleanly (uptime=%s)", time.Since(startedAt).Round(time.Millisecond))
	return nil
}

// cleanupResources 释放 wire 装配的资源（DB 连接池、PG 监听等），整体限时：
// 任一资源 Close 卡死都不得拖住进程退出，超时即放弃并告警（各步骤自身也有界）。
func cleanupResources(cleanup func(), cfg config.Config) {
	if cleanup == nil {
		return
	}
	timeout := cfg.Server.ShutdownTimeout
	if timeout <= 0 {
		timeout = server.DefaultShutdownTimeout
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		cleanup()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("stateflux: [cleanup] exceeded total budget %s; exiting anyway", timeout)
	}
}

// shutdownContext 返回首个 SIGINT/SIGTERM 到达时取消的 context，并在再次收到信号时强制退出：
// 优雅退出卡住时给运维一个确定的逃生口，避免关机无限挂起。
func shutdownContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go superviseSignals(ctx, cancel, sigCh, os.Exit)
	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}

// superviseSignals 实现两阶段信号语义：首个信号取消 ctx 触发优雅退出；再次收到信号
// 调用 exit 强制结束（独立成函数以便测试，不依赖真实进程信号）。
func superviseSignals(ctx context.Context, cancel context.CancelFunc, sigCh <-chan os.Signal, exit func(int)) {
	select {
	case sig := <-sigCh:
		log.Printf("stateflux: [signal] received %s; shutting down gracefully (repeat signal to force exit)", sig)
		cancel()
	case <-ctx.Done():
		return
	}
	sig := <-sigCh
	log.Printf("stateflux: [signal] received %s again; forcing exit", sig)
	exit(2)
}

// flushObs 在优雅退出预算内刷出未发送的 OTel 指标/链路；失败只记录，不阻塞退出。
func flushObs(shutdown func(context.Context) error, cfg config.Config) {
	if shutdown == nil {
		return
	}
	timeout := cfg.Server.ShutdownTimeout
	if timeout <= 0 {
		timeout = server.DefaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Printf("stateflux: [obs] flush failed: %v", err)
	}
}
