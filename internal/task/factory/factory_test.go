package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/internal/task"
)

type stubFactory struct {
	name     string
	interval time.Duration
	tasks    []task.Task
	err      error
}

func (f *stubFactory) Name() string            { return f.name }
func (f *stubFactory) Interval() time.Duration { return f.interval }
func (f *stubFactory) Generate(context.Context, time.Time) ([]task.Task, error) {
	return f.tasks, f.err
}

func TestRegistryRegisterValidation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(nil); err == nil {
		t.Fatal("nil factory must be rejected")
	}
	if err := reg.Register(&stubFactory{name: ""}); err == nil {
		t.Fatal("empty name must be rejected")
	}
	if err := reg.Register(&stubFactory{name: "a"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Register(&stubFactory{name: "a"}); err == nil {
		t.Fatal("duplicate name must be rejected")
	}
	if _, ok := reg.Get("a"); !ok {
		t.Fatal("get must find registered factory")
	}
	if all := reg.All(); len(all) != 1 || all[0].Name() != "a" {
		t.Fatalf("all = %+v", all)
	}
}

func TestManagerRunOnceAggregatesErrors(t *testing.T) {
	ok := &stubFactory{name: "ok", tasks: []task.Task{task.FuncTask{}, task.FuncTask{}}}
	bad := &stubFactory{name: "bad", err: errors.New("generate failed")}
	reg := NewRegistry()
	_ = reg.Register(bad)
	_ = reg.Register(ok)

	var sunk []task.Task
	m := NewManager(reg, func(context.Context, task.Task) error {
		return nil
	})
	m.sink = func(_ context.Context, tt task.Task) error { sunk = append(sunk, tt); return nil }

	enqueued, err := m.RunOnce(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "generate failed") {
		t.Fatalf("err = %v, want aggregated factory error", err)
	}
	if enqueued != 2 || len(sunk) != 2 {
		t.Fatalf("enqueued = %d, sunk = %d; good factory must still enqueue", enqueued, len(sunk))
	}
}

func TestManagerRunOnceFactorySinkError(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&stubFactory{name: "a", tasks: []task.Task{task.FuncTask{}}})
	m := NewManager(reg, func(context.Context, task.Task) error { return errors.New("pg down") })

	if _, err := m.RunOnceFactory(context.Background(), reg.All()[0], time.Now()); err == nil {
		t.Fatal("sink error must surface")
	}
}

func TestManagerRunReportsErrorsToHook(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&stubFactory{name: "loop", interval: 5 * time.Millisecond, err: errors.New("always fails")})

	errs := make(chan error, 1)
	m := NewManager(reg, func(context.Context, task.Task) error { return nil })
	m.WithOnError(func(name string, err error) {
		if name == "loop" {
			select {
			case errs <- err:
			default:
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go m.Run(ctx)

	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "always fails") {
			t.Fatalf("hook error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("on-error hook never fired; failures must not be silently dropped")
	}
}
