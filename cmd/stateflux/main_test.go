package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSuperviseSignalsGracefulThenForce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 2)
	exits := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseSignals(ctx, cancel, sigCh, func(code int) { exits <- code })
	}()

	// 首个信号：只取消 context（触发优雅退出），不强制退出。
	sigCh <- syscall.SIGTERM
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("first signal did not cancel context")
	}
	select {
	case code := <-exits:
		t.Fatalf("unexpected force exit(%d) after first signal", code)
	default:
	}

	// 第二个信号：强制退出（码 2）。
	sigCh <- syscall.SIGINT
	select {
	case code := <-exits:
		if code != 2 {
			t.Fatalf("force exit code = %d, want 2", code)
		}
	case <-time.After(time.Second):
		t.Fatal("second signal did not force exit")
	}
	<-done
}

func TestSuperviseSignalsStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseSignals(ctx, func() {}, make(chan os.Signal), func(int) {})
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("superviseSignals did not return after context cancellation")
	}
}
