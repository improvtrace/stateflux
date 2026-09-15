package worker

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// recordingWAL 记录 Add/Close 的先后顺序，用于验证优雅退出时「先写完在途结果，再关闭 WAL」。
type recordingWAL struct {
	*MemWAL

	mu     sync.Mutex
	events []string
}

func newRecordingWAL() *recordingWAL {
	return &recordingWAL{MemWAL: NewWAL(DefaultWALMax)}
}

func (w *recordingWAL) record(ev string) {
	w.mu.Lock()
	w.events = append(w.events, ev)
	w.mu.Unlock()
}

func (w *recordingWAL) Add(ev *taskv1.ResultEvent) {
	w.record("add")
	w.MemWAL.Add(ev)
}

func (w *recordingWAL) Close() error {
	w.record("close")
	return w.MemWAL.Close()
}

func (w *recordingWAL) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

func TestRuntimeStopDrainsInflightExecution(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	handlers := NewHandlerRegistry()
	if err := handlers.Register(HandlerFunc{
		T: "demo", O: "slow",
		F: func(context.Context, *taskv1.TaskMessage) (Result, error) {
			close(entered)
			<-release
			return Result{Outcome: taskv1.Outcome_OUTCOME_SUCCEEDED}, nil
		},
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	wal := newRecordingWAL()
	rt := NewRuntime(RuntimeOptions{Handlers: handlers, NodeID: "n1", WAL: wal})

	execDone := make(chan struct{})
	go func() {
		rt.Execute(context.Background(), &taskv1.TaskMessage{TaskId: 1, Type: "demo", Operator: "slow"})
		close(execDone)
	}()
	<-entered

	stopDone := make(chan error, 1)
	go func() { stopDone <- rt.Stop() }()

	// Stop 必须等待在途执行结束，期间不得返回。
	select {
	case <-stopDone:
		t.Fatal("Stop returned while an execution was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-execDone:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight execution did not finish")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the in-flight execution finished")
	}

	if got, want := wal.snapshot(), []string{"add", "close"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("WAL events = %v, want %v", got, want)
	}
}
