package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/improvtrace/stateflux/internal/config"
)

// Component 是随服务启停的运行时组件（worker/调度/归集/对账/共识同步/工厂等）。
type Component interface {
	// Start 启动组件（非阻塞）。
	Start(ctx context.Context) error
	// Stop 停止组件（幂等）。
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

	mu      sync.Mutex
	started bool
}

// AppOptions 是 App 的装配参数。
type AppOptions struct {
	Config     config.Config
	GRPCServer *grpc.Server
	HTTPServer *http.Server
	Components []Component
	Closers    []func()
}

// NewApp 构造服务。cfg.Server.GRPCAddr 与 cfg.Server.HTTPAddr 决定监听地址。
func NewApp(opts AppOptions) *App {
	app := &App{
		cfg:        opts.Config,
		grpcServer: opts.GRPCServer,
		httpServer: opts.HTTPServer,
		components: opts.Components,
		closers:    opts.Closers,
	}
	return app
}

// Run 启动监听与全部组件，阻塞直到 ctx 结束，然后优雅关闭。
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

	errCh := make(chan error, 1)
	go func() {
		if serveErr := a.grpcServer.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			errCh <- serveErr
		}
	}()

	if a.httpServer != nil {
		go func() {
			if serveErr := a.httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				errCh <- serveErr
			}
		}()
	}

	started := make([]Component, 0, len(a.components))
	for _, c := range a.components {
		if err := c.Start(ctx); err != nil {
			a.shutdown()
			return fmt.Errorf("server: start component: %w", err)
		}
		started = append(started, c)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		a.stopComponents(started)
		a.shutdown()
		return err
	}
	a.stopComponents(started)
	a.shutdown()
	return nil
}

func (a *App) stopComponents(components []Component) {
	// 逆序停止，先停消费者再停生产者。
	for i := len(components) - 1; i >= 0; i-- {
		_ = components[i].Stop()
	}
}

func (a *App) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout)
	defer cancel()
	if a.httpServer != nil {
		_ = a.httpServer.Shutdown(ctx)
	}
	if a.grpcServer != nil {
		done := make(chan struct{})
		go func() {
			a.grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			a.grpcServer.Stop()
		}
	}
	for _, closer := range a.closers {
		closer()
	}
}

// NewHealthMux 构造健康检查与诊断 HTTP 路由。
func NewHealthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}

// NewHTTPServer 构造健康 HTTP 服务。
func NewHTTPServer(cfg config.Config) *http.Server {
	return &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           NewHealthMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
}
