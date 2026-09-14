// Command stateflux 是服务入口（§8、§15.1#1）：flag/env → config → obs 装配 →
// internal/server 的 wire 注入 → 启动。
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/obs"
	"github.com/improvtrace/stateflux/internal/server"
)

func main() {
	cfg := config.FromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownObs, err := obs.Setup(ctx, cfg.Obs)
	if err != nil {
		log.Fatalf("stateflux: obs setup: %v", err)
	}
	defer func() {
		if serr := shutdownObs(context.Background()); serr != nil {
			log.Printf("stateflux: obs shutdown: %v", serr)
		}
	}()

	// 启动依赖 wire 注入（§15.1#1）：全部构造与依赖关系由 wire_gen.go 决定。
	app, cleanup, err := server.InitializeApplication(ctx, cfg)
	if err != nil {
		log.Fatalf("stateflux: wire init: %v", err)
	}
	defer cleanup()

	log.Printf("stateflux starting: node=%s grpc=%s http=%s cluster=%s triggers=%v queues=%v",
		cfg.Node.NodeID, cfg.Server.GRPCAddr, cfg.Server.HTTPAddr,
		cfg.Cluster.Transport, cfg.Runtime.SchedulerTriggers, cfg.Runtime.WorkerQueues)

	if err := app.Run(ctx); err != nil {
		log.Fatalf("stateflux: run: %v", err)
	}
	log.Printf("stateflux stopped")
}
