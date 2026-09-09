// gRPC，全角色装配（api/scheduler/executor/collector/reconcile），验证设计 §4 状态机、
// §5 四阶段流程与 §6 关键机制的收敛行为。
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	apiv1 "github.com/improvtrace/stateflux/proto/gen/apiv1"
	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"

	"github.com/improvtrace/stateflux/api"
	"github.com/improvtrace/stateflux/cluster"
	"github.com/improvtrace/stateflux/collector"
	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/executor"
	"github.com/improvtrace/stateflux/internal/testpg"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/reconcile"
	"github.com/improvtrace/stateflux/scheduler"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

var testDSN string

func TestMain(m *testing.M) {
	dsn, err := testpg.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testDSN = dsn
	code := m.Run()
	testpg.Stop()
	os.Exit(code)
}

// ---- 可靠性实验台的 handler 行为 ----

type handlerPlan struct {
	mu         sync.Mutex
	failFirstN map[string]int // task type → 前 N 次执行失败
	executions map[string]int // task type → 执行总次数
}

func (h *handlerPlan) exec(taskType string, fn func() ([]byte, error)) ([]byte, error) {
	h.mu.Lock()
	h.executions[taskType]++
	n := h.executions[taskType]
	failN := h.failFirstN[taskType]
	h.mu.Unlock()
	if n <= failN {
		return nil, fmt.Errorf("planned failure #%d", n)
	}
	return fn()
}

// fixture 单机闭环全家桶。
type fixture struct {
	t          *testing.T
	st         store.Store
	q          *queue.Queue
	mr         *miniredis.Miniredis
	api        *api.Service
	reg        *sdk.Registry
	plan       *handlerPlan
	leadCancel context.CancelFunc
	leadDone   <-chan struct{}
	execDone   <-chan struct{}
	collector  *collector.Collector
	pool       *executor.Pool
}

func truncate(t *testing.T) {
	t.Helper()
	testpg.Truncate(t)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	metrics, err := obs.New()
	if err != nil {
		t.Fatal(err)
	}
	sf, err := sdk.NewSnowflake(7)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(store.Options{DSN: testDSN, Snowflake: sf, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	truncate(t)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)

	// executor：进程内 gRPC（临时端口）。
	walDir := t.TempDir()
	wal, err := executor.OpenWAL(executor.WALConfig{Dir: walDir, MaxEntries: 1000, MaxBytes: 1 << 20, SegmentBytes: 1 << 18}, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	plan := &handlerPlan{failFirstN: map[string]int{}, executions: map[string]int{}}
	reg := sdk.NewRegistry()
	reg.Register(&sdk.HandlerFunc{HandlerType: "work", Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
		return plan.exec("work", func() ([]byte, error) {
			return []byte(fmt.Sprintf(`{"done":%d}`, task.ID)), nil
		})
	}})
	reg.Register(&sdk.HandlerFunc{HandlerType: "fail", Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
		return plan.exec("fail", func() ([]byte, error) { return nil, fmt.Errorf("always fails") })
	}})
	executorOpts := executor.Options{
		NodeID: "node-1", Registry: reg, Queue: q, WAL: wal,
		Concurrency: 8, LeaseTTL: 5 * time.Second, LeaseRenewInterval: 1 * time.Second,
		BRPOPTimeout: 100 * time.Millisecond, CapacityReportInterval: 200 * time.Millisecond,
		Metrics: metrics, Logger: log,
	}
	exec, err := executor.New(executorOpts)
	if err != nil {
		t.Fatal(err)
	}
	execSrv := grpc.NewServer()
	dispatchAddr := startGRPC(t, execSrv, func(s *grpc.Server) {
		dispatchv1.RegisterExecutorServiceServer(s, executor.NewServer(exec, log))
	})
	execCtx, execCancel := context.WithCancel(context.Background())
	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		_ = exec.Run(execCtx)
	}()
	t.Cleanup(func() {
		execCancel()
		select {
		case <-execDone:
		case <-time.After(10 * time.Second):
			t.Log("executor shutdown timeout")
		}
	})

	view := cluster.NewStatic(cluster.StaticConfig{NodeID: "sched-1", Address: dispatchAddr})
	pool := executor.NewPool()
	t.Cleanup(func() { _ = pool.Close() })

	// 调度侧角色。
	fast := config.Config{}
	fast.ApplyDefaults()
	fast.Scheduler.TickInterval = 50 * time.Millisecond
	fast.Scheduler.PromoterInterval = 50 * time.Millisecond
	fast.Collector.PullInterval = 50 * time.Millisecond
	fast.Collector.FlushInterval = 50 * time.Millisecond
	fast.Collector.FlushCount = 10
	fast.Reconcile.Interval = 200 * time.Millisecond
	fast.Reconcile.Grace = 300 * time.Millisecond

	coll := collector.New(collector.Options{Store: st, Queue: q, View: view, Pool: pool, Cfg: fast.Collector, Metrics: metrics, Logger: log})
	sched, err := scheduler.New(scheduler.Options{
		NodeID: "sched-1", Store: st, Queue: q, View: view, Pool: pool,
		Sink: coll, Cfg: fast.Scheduler, Metrics: metrics, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	promoter := scheduler.NewPromoter(scheduler.PromoterOptions{Store: st, Interval: fast.Scheduler.PromoterInterval, Batch: 100, Metrics: metrics, Logger: log})
	rec := reconcile.New(reconcile.Options{Store: st, Queue: q, Cfg: fast.Reconcile, Metrics: metrics, Logger: log})

	leadCtx, leadCancel := context.WithCancel(context.Background())
	leadDone := make(chan struct{})
	go func() {
		defer close(leadDone)
		var wg sync.WaitGroup
		wg.Add(4)
		go func() { defer wg.Done(); promoter.Run(leadCtx, nil) }()
		go func() { defer wg.Done(); sched.Run(leadCtx, nil) }()
		go func() { defer wg.Done(); coll.Run(leadCtx) }()
		go func() { defer wg.Done(); rec.Run(leadCtx) }()
		wg.Wait()
	}()
	t.Cleanup(func() {
		leadCancel()
		select {
		case <-leadDone:
		case <-time.After(10 * time.Second):
			t.Log("leader roles shutdown timeout")
		}
	})

	return &fixture{
		t: t, st: st, q: q, mr: mr,
		api:        api.New(st, config.APIConfig{}, log),
		reg:        reg,
		plan:       plan,
		leadCancel: leadCancel,
		leadDone:   leadDone,
		execDone:   execDone,
		collector:  coll,
		pool:       pool,
	}
}

func startGRPC(t *testing.T, srv *grpc.Server, register func(*grpc.Server)) string {
	t.Helper()
	register(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// createTasks 经 API 角色（模式 B RPC 语义，走同一 store 路径）批量创建。
func (f *fixture) createTasks(items ...sdk.NewTask) []int64 {
	f.t.Helper()
	resp, err := f.api.CreateTasks(context.Background(), &apiv1.CreateTasksRequest{
		Items: toProtoItems(items),
	})
	if err != nil {
		f.t.Fatal(err)
	}
	ids := make([]int64, 0, len(resp.Tasks))
	for _, c := range resp.Tasks {
		ids = append(ids, c.TaskId)
	}
	return ids
}

// waitResults 轮询等待全部终态（长轮询 API 语义）。
func (f *fixture) waitResults(ids []int64, timeout time.Duration) map[int64]*sdk.TaskResult {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := f.api.GetResults(context.Background(), &apiv1.GetResultsRequest{TaskIds: ids, WaitMs: 200})
		if err != nil {
			f.t.Fatal(err)
		}
		all := true
		for _, r := range resp.Results {
			if r.Outcome == "" {
				all = false
				break
			}
		}
		if all {
			out := make(map[int64]*sdk.TaskResult, len(ids))
			for _, r := range resp.Results {
				out[r.TaskId] = &sdk.TaskResult{
					TaskID: r.TaskId, Outcome: sdk.Outcome(r.Outcome), Attempt: r.Attempt,
					Result: r.Result, Error: r.Error,
					CompletedAt: time.UnixMilli(r.CompletedAtUnixMs),
				}
			}
			return out
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("tasks not terminal within %v: %+v", timeout, resp.Results)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func toProtoItems(items []sdk.NewTask) []*apiv1.CreateTaskItem {
	out := make([]*apiv1.CreateTaskItem, 0, len(items))
	for _, it := range items {
		item := &apiv1.CreateTaskItem{
			Type: it.Type, Payload: it.Payload,
			Priority: string(it.Priority), ExecMode: string(it.ExecMode),
			TimeoutMs: it.TimeoutMS, MaxAttempts: it.MaxAttempts,
			IdempotencyKey: it.IdempotencyKey, BatchId: it.BatchID,
		}
		if !it.RunAt.IsZero() {
			item.RunAtUnixMs = it.RunAt.UnixMilli()
		}
		if it.Callback != nil {
			raw, _ := json.Marshal(it.Callback)
			item.CallbackJson = raw
		}
		out = append(out, item)
	}
	return out
}
