// Command stateflux 是服务入口（§8、§15.1#1）：flag/env → config → obs 装配 →
// internal/server 的 wire 注入 → 启动；收到 SIGINT/SIGTERM 后优雅退出，再次收到信号强制退出。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/server"
)

func main() {
	os.Exit(run())
}

// run 执行一次完整生命周期并返回进程退出码。所有资源释放都在本函数内显式完成：
// 不使用 log.Fatal/os.Exit 提前返回，避免跳过 DB 关闭与 OTel 数据刷出。
func run() int {
	cfg := config.FromEnv()

	ctx, stopSignals := shutdownContext()
	defer stopSignals()

	shutdownObs, err := obs.Setup(ctx, cfg.Obs)
	if err != nil {
		log.Printf("stateflux: obs setup: %v", err)
		return 1
	}

	// 启动依赖 wire 注入（§15.1#1）：全部构造与依赖关系由 wire_gen.go 决定。
	app, cleanup, err := server.InitializeApplication(ctx, cfg)
	if err != nil {
		log.Printf("stateflux: wire init: %v", err)
		flushObs(shutdownObs, cfg)
		return 1
	}

	log.Printf("stateflux starting: node=%s grpc=%s http=%s cluster=%s triggers=%v queues=%v",
		cfg.Node.NodeID, cfg.Server.GRPCAddr, cfg.Server.HTTPAddr,
		cfg.Cluster.Transport, cfg.Runtime.SchedulerTriggers, cfg.Runtime.WorkerQueues)

	// Run 在信号到达时返回：内部先停组件与服务面，随后这里按装配顺序逆序释放资源。
	runErr := app.Run(ctx)
	cleanupResources(cleanup, cfg)
	flushObs(shutdownObs, cfg)

	if runErr != nil {
		log.Printf("stateflux: run: %v", runErr)
		return 1
	}
	log.Printf("stateflux stopped")
	return 0
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
		log.Printf("stateflux: resource cleanup exceeded %s; exiting anyway", timeout)
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
		log.Printf("stateflux: received %s, shutting down gracefully (repeat signal to force exit)", sig)
		cancel()
	case <-ctx.Done():
		return
	}
	sig := <-sigCh
	log.Printf("stateflux: received %s again, forcing exit", sig)
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
		log.Printf("stateflux: obs shutdown: %v", err)
	}
}
