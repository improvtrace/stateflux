package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/improvtrace/stateflux/internal/config"
)

// DefaultShutdownTimeout 是未配置（或配置非法）时的优雅退出等待上限。
const DefaultShutdownTimeout = 10 * time.Second

// Component 是随服务启停的运行时组件（worker/调度/归集/对账/共识同步/工厂等）。
type Component interface {
	// Start 启动组件（非阻塞）。
	Start(ctx context.Context) error
	// Stop 停止组件（幂等）。
	//
	// Stop 不接受 context：优雅退出的整体预算由 App 用 ShutdownTimeout 约束；
	// App 在独立 goroutine 中调用 Stop 并在超时后放弃等待，避免单个组件卡死进程。
	Stop() error
}

// ComponentFunc 是 Component 的函数适配器。
type ComponentFunc struct {
	StartFn func(ctx context.Context) error
	StopFn  func() error
}

// Start 实现 Component。
func (c ComponentFunc) Start(ctx context.Context) error {
	if c.StartFn == nil {
		return nil
	}
	return c.StartFn(ctx)
}

// Stop 实现 Component。
func (c ComponentFunc) Stop() error {
	if c.StopFn == nil {
		return nil
	}
	return c.StopFn()
}

// App 是装配后的服务：gRPC 服务面 + 健康 HTTP + 一组运行时组件。
type App struct {
	cfg        config.Config
	grpcServer *grpc.Server
	httpServer *http.Server
	components []Component
	closers    []func()
	health     *Health

	mu      sync.Mutex
	started bool
}

// AppOptions 是 App 的装配参数。
type AppOptions struct {
	Config     config.Config
	GRPCServer *grpc.Server
	HTTPServer *http.Server
	Components []Component
	// Closers 是组件与服务面停止后逆序执行的清理函数（DB 等底层资源）。
	Closers []func()
	// Health 暴露就绪状态；nil 时退化为 NewHealth()。
	Health *Health
}

// NewApp 构造服务。cfg.Server.GRPCAddr 与 cfg.Server.HTTPAddr 决定监听地址。
func NewApp(opts AppOptions) *App {
	health := opts.Health
	if health == nil {
		health = NewHealth()
	}
	return &App{
		cfg:        opts.Config,
		grpcServer: opts.GRPCServer,
		httpServer: opts.HTTPServer,
		components: opts.Components,
		closers:    opts.Closers,
		health:     health,
	}
}

// Run 启动监听与全部组件，阻塞直到 ctx 结束或服务面出错，随后优雅关闭。
//
// 关停顺序（总预算由 cfg.Server.ShutdownTimeout 约束）：
//  1. /readyz 转 503，通知上游摘除流量；
//  2. gRPC GracefulStop（停止接入新 RPC、排空在途）+ HTTP Shutdown（停止接入新请求）；
//  3. 逆序停止后台组件，让在途执行与循环收尾；
//  4. 等待服务面排空；超出预算则强制 grpc.Stop，返回超时错误；
//  5. 逆序执行清理函数（DB 等）。
//
// 任一阶段出错都不会中断后续阶段：错误聚合返回，确保资源总是被释放。
func (a *App) Run(ctx context.Context) error {
	if a.grpcServer == nil {
		return errors.New("server: nil grpc server")
	}
	lis, err := net.Listen("tcp", a.cfg.Server.GRPCAddr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", a.cfg.Server.GRPCAddr, err)
	}

	a.mu.Lock()
	a.started = true
	a.mu.Unlock()

	serveErrCh := make(chan error, 2)
	go func() {
		if serveErr := a.grpcServer.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			serveErrCh <- serveErr
		}
	}()
	if a.httpServer != nil {
		go func() {
			if serveErr := a.httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				serveErrCh <- serveErr
			}
		}()
	}

	// 启动失败必须回滚已启动的组件，再关闭服务面，不允许半启动状态泄漏。
	started, startErr := a.startComponents(ctx)
	if startErr != nil {
		a.health.SetReady(false)
		return errors.Join(startErr, a.shutdown(started))
	}
	a.health.SetReady(true)
	log.Printf("stateflux: [ready] serving grpc=%s http=%s components=%d", a.cfg.Server.GRPCAddr, a.cfg.Server.HTTPAddr, len(started))

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serveErrCh:
	}
	a.health.SetReady(false)
	log.Printf("stateflux: [shutdown] draining connections (budget=%s)", a.shutdownTimeout())
	return errors.Join(runErr, a.shutdown(started))
}

// startComponents 按声明顺序启动组件，返回已成功启动的组件以便失败时回滚。
func (a *App) startComponents(ctx context.Context) ([]Component, error) {
	started := make([]Component, 0, len(a.components))
	for _, c := range a.components {
		if err := c.Start(ctx); err != nil {
			return started, fmt.Errorf("server: start component: %w", err)
		}
		started = append(started, c)
	}
	return started, nil
}

// shutdownTimeout 返回有效的优雅退出预算。
func (a *App) shutdownTimeout() time.Duration {
	if a.cfg.Server.ShutdownTimeout > 0 {
		return a.cfg.Server.ShutdownTimeout
	}
	return DefaultShutdownTimeout
}

// shutdown 在预算内完成全部关闭步骤，聚合各阶段错误。
func (a *App) shutdown(components []Component) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout())
	defer cancel()

	var errs []error

	// 1. 停止接入新请求：gRPC 进入 graceful stop（停止接入、等待在途 RPC）；HTTP 停止监听
	//    并排空在途请求。二者并行，互不阻塞。
	grpcDone := make(chan struct{})
	if a.grpcServer != nil {
		go func() {
			a.grpcServer.GracefulStop()
			close(grpcDone)
		}()
	} else {
		close(grpcDone)
	}
	httpDone := make(chan error, 1)
	if a.httpServer != nil {
		go func() { httpDone <- a.httpServer.Shutdown(ctx) }()
	} else {
		httpDone <- nil
	}

	// 2. 逆序停止后台组件，与上面共用同一 deadline。
	if err := a.stopComponents(components, ctx); err != nil {
		errs = append(errs, err)
	}

	// 3. 等待服务面排空。长连接流（如 ResultStream）可能不会自行结束：超出预算即强制关闭，
	//    这是既定兜底而非错误，仅告警；组件停不下来才是需要上报的失败（见步骤 2）。
	select {
	case <-grpcDone:
	case <-ctx.Done():
		if a.grpcServer != nil {
			log.Printf("stateflux: [shutdown] grpc graceful stop exceeded %s; forcing stop", a.shutdownTimeout())
			a.grpcServer.Stop()
		}
	}
	if err := <-httpDone; err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("stateflux: [shutdown] http shutdown exceeded %s; forcing close", a.shutdownTimeout())
			if a.httpServer != nil {
				_ = a.httpServer.Close()
			}
		} else {
			errs = append(errs, fmt.Errorf("server: http shutdown: %w", err))
		}
	}

	// 4. 逆序释放底层资源；即使前面超时也要执行。
	for i := len(a.closers) - 1; i >= 0; i-- {
		if a.closers[i] != nil {
			a.closers[i]()
		}
	}
	return errors.Join(errs...)
}

// stopComponents 逆序停止组件，逐组件受剩余预算约束。
func (a *App) stopComponents(components []Component, ctx context.Context) error {
	var errs []error
	for i := len(components) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			errs = append(errs, fmt.Errorf("server: stop %T skipped: %w", components[i], ctx.Err()))
			continue
		}
		if err := stopComponent(ctx, components[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// stopComponent 调用 c.Stop 并在预算耗尽时放弃等待（组件 goroutine 随进程退出回收）。
func stopComponent(ctx context.Context, c Component) error {
	done := make(chan error, 1)
	go func() { done <- c.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("server: stop %T: %w", c, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("server: stop %T: %w", c, ctx.Err())
	}
}

// NewHTTPServer 构造健康检查 HTTP 服务。
func NewHTTPServer(cfg config.Config, health *Health) *http.Server {
	if health == nil {
		health = NewHealth()
	}
	return &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           NewHealthMux(health),
		ReadHeaderTimeout: 5 * time.Second,
	}
}
