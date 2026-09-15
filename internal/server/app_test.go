package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/improvtrace/stateflux/internal/config"
)

// recorder 记录组件启停顺序（并发安全）。
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.order = append(r.order, s)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func testServerConfig() config.Config {
	cfg := config.Default()
	cfg.Server.GRPCAddr = "127.0.0.1:0"
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Server.ShutdownTimeout = 2 * time.Second
	return cfg
}

func TestAppGracefulShutdownReverseOrder(t *testing.T) {
	rec := &recorder{}
	components := []Component{
		ComponentFunc{
			StartFn: func(context.Context) error { rec.add("start:a"); return nil },
			StopFn:  func() error { rec.add("stop:a"); return nil },
		},
		ComponentFunc{
			StartFn: func(context.Context) error { rec.add("start:b"); return nil },
			StopFn:  func() error { rec.add("stop:b"); return nil },
		},
	}

	health := NewHealth()
	cfg := testServerConfig()
	app := NewApp(AppOptions{
		Config:     cfg,
		GRPCServer: grpc.NewServer(),
		HTTPServer: NewHTTPServer(cfg, health),
		Components: components,
		Health:     health,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()

	waitUntil(t, func() bool { return len(rec.snapshot()) >= 2 })
	if !health.Ready() {
		t.Fatal("health should be ready after all components start")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if got, want := rec.snapshot(), []string{"start:a", "start:b", "stop:b", "stop:a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lifecycle order = %v, want %v", got, want)
	}
	if health.Ready() {
		t.Fatal("health must be not-ready after shutdown")
	}
}

func TestAppStartFailureRollsBackStartedComponents(t *testing.T) {
	var (
		started int
		stopped int
	)
	components := []Component{
		ComponentFunc{
			StartFn: func(context.Context) error { started++; return nil },
			StopFn:  func() error { stopped++; return nil },
		},
		ComponentFunc{
			StartFn: func(context.Context) error { return errors.New("boom") },
		},
	}

	cfg := testServerConfig()
	app := NewApp(AppOptions{
		Config:     cfg,
		GRPCServer: grpc.NewServer(),
		HTTPServer: NewHTTPServer(cfg, NewHealth()),
		Components: components,
	})

	done := make(chan error, 1)
	go func() { done <- app.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("run error = %v, want boom", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after component start failure")
	}
	if started != 1 || stopped != 1 {
		t.Fatalf("started=%d stopped=%d, want 1/1 (started components must be rolled back)", started, stopped)
	}
}

func TestAppShutdownTimeoutBoundsBlockingStop(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	started := make(chan struct{})
	components := []Component{
		ComponentFunc{
			StartFn: func(context.Context) error { close(started); return nil },
			// 模拟无法在预算内退出的组件：Stop 永久阻塞。
			StopFn: func() error { <-block; return nil },
		},
	}

	cfg := testServerConfig()
	cfg.Server.ShutdownTimeout = 100 * time.Millisecond
	app := NewApp(AppOptions{
		Config:     cfg,
		GRPCServer: grpc.NewServer(),
		HTTPServer: NewHTTPServer(cfg, NewHealth()),
		Components: components,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	<-started

	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("run error = %v, want deadline exceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run ignored ShutdownTimeout and blocked on a stuck component")
	}
}

func TestHealthMuxReadiness(t *testing.T) {
	health := NewHealth()
	mux := NewHealthMux(health)

	status := func(path string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready = %d, want 503", got)
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", got)
	}
	health.SetReady(true)
	if got := status("/readyz"); got != http.StatusOK {
		t.Fatalf("/readyz when ready = %d, want 200", got)
	}
	health.SetReady(false)
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while draining = %d, want 503", got)
	}
}
