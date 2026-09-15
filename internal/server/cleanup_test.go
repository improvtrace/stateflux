package server

import (
	"testing"
	"time"
)

func TestBoundedCleanupRunsStep(t *testing.T) {
	ran := false
	boundedCleanup("fast", time.Second, func() { ran = true })()
	if !ran {
		t.Fatal("cleanup step did not run")
	}
}

func TestBoundedCleanupAbandonsBlockingStep(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	called := make(chan struct{})

	cleanup := boundedCleanup("blocking", 50*time.Millisecond, func() {
		close(called)
		<-release // 模拟底层 Close 卡死
	})

	start := time.Now()
	cleanup()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("boundedCleanup blocked for %s, want bounded wait", elapsed)
	}
	<-called
}
