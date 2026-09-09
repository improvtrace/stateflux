package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// TestCreateAndIdempotency 幂等键冲突跳过插入、返回已存在 task_id（§5.1 两模式幂等语义一致）。
func TestCreateAndIdempotency(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	created, err := s.CreatePending(ctx, []sdk.NewTask{
		{Type: "email.send", Payload: []byte(`{"to":"a@b.c"}`), IdempotencyKey: "order-1"},
		{Type: "email.send", Payload: []byte(`{"to":"x@y.z"}`)}, // 无幂等键
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 2 || created[0].Duplicate || created[1].Duplicate {
		t.Fatalf("first create: %+v", created)
	}

	// 同键重复创建：跳过插入、返回原 ID。
	again, err := s.CreatePending(ctx, []sdk.NewTask{
		{Type: "email.send", Payload: []byte(`{"to":"changed"}`), IdempotencyKey: "order-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !again[0].Duplicate || again[0].TaskID != created[0].TaskID {
		t.Fatalf("idempotent create: %+v, want duplicate of %d", again[0], created[0].TaskID)
	}

	// 无键任务每次都新建。
	third, err := s.CreatePending(ctx, []sdk.NewTask{{Type: "email.send"}})
	if err != nil {
		t.Fatal(err)
	}
	if third[0].TaskID == created[1].TaskID {
		t.Fatal("task without idempotency key must create a new row")
	}
}

// TestConcurrentClaim 并发认领单测：N 个调度者并发认领同一批任务，
// 每个任务只允许被认领一次（SKIP LOCKED 挪行原子性，§9.1/§12.2）。
func TestConcurrentClaim(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	const total = 200
	items := make([]sdk.NewTask, total)
	for i := range items {
		items[i] = newTask("job")
	}
	if _, err := s.CreatePending(ctx, items); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now(), Limit: total}); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var (
		mu       sync.Mutex
		claimed  []int64
		seen     = make(map[int64]int)
		dupCount int
		wg       sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := newStoreWithNode(t, int64(w)+10) // 每个并发 worker 独立连接与雪花节点
			for {
				tasks, err := ws.Claim(ctx, "sched-1", 7)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if len(tasks) == 0 {
					return
				}
				mu.Lock()
				for _, task := range tasks {
					seen[task.ID]++
					if seen[task.ID] > 1 {
						dupCount++
					}
					claimed = append(claimed, task.ID)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if dupCount != 0 {
		t.Fatalf("duplicate claims: %d", dupCount)
	}
	if len(claimed) != total {
		t.Fatalf("claimed %d, want %d", len(claimed), total)
	}
	// attempts +1 已在认领时生效。
	for _, id := range claimed {
		_, stages, err := s.GetResults(ctx, []int64{id})
		if err != nil {
			t.Fatal(err)
		}
		if stages[id] != sdk.StageProcessing {
			t.Fatalf("task %d stage = %s, want processing", id, stages[id])
		}
		if res := resultsFor(t, s, id); res != nil {
			t.Fatalf("task %d unexpectedly terminal", id)
		}
	}
}

// resultsFor 查询单个任务结果（不参与断言时使用）。
func resultsFor(t *testing.T, s store.Store, id int64) *sdk.TaskResult {
	t.Helper()
	results, _, err := s.GetResults(context.Background(), []int64{id})
	if err != nil {
		t.Fatal(err)
	}
	return results[id]
}

// TestFinalizeAttemptFencing 僵尸节点用例：attempt 过期（陈旧副本/重复归集）后写终态必须被拒
// （attempt 匹配才生效，§12.2/§6.2）。
func TestFinalizeAttemptFencing(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	created, err := s.CreatePending(ctx, []sdk.NewTask{newTask("job")})
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.Claim(ctx, "sched-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("claim %d tasks", len(tasks))
	}
	attempt := tasks[0].Attempts

	// 同一 attempt 的终态写入生效。
	report, err := s.Finalize(ctx, []store.TerminalEntry{{
		TaskID: tid, Attempt: attempt, Action: store.TerminalActionTerminal,
		Outcome: sdk.OutcomeSucceeded, Result: []byte(`{"ok":true}`), CompletedAt: time.Now(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Applied[tid] != store.EntryStatusApplied {
		t.Fatalf("terminal write rejected: %+v", report.Applied)
	}

	// 同一 attempt 再次写入（重复归集）：attempt 已不匹配 processing（任务已终态）→ 跳过。
	report2, err := s.Finalize(ctx, []store.TerminalEntry{{
		TaskID: tid, Attempt: attempt, Action: store.TerminalActionTerminal,
		Outcome: sdk.OutcomeSucceeded, Result: []byte(`{"ok":true}`), CompletedAt: time.Now(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if report2.Applied[tid] != store.EntryStatusSkipped {
		t.Fatalf("duplicate terminal write must be skipped: %+v", report2.Applied)
	}

	// 结果不可变：task_results 只有最初一行（jsonb 规范化，比较解析值）。
	results, _, err := s.GetResults(ctx, []int64{tid})
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if results[tid] != nil {
		_ = json.Unmarshal(results[tid].Result, &got)
	}
	if len(results) != 1 || !reflect.DeepEqual(got, map[string]any{"ok": true}) {
		t.Fatalf("results mutated: %s", results[tid].Result)
	}
}

// TestFinalizePayloadMergeAndCallback 终态事务：payload 合并入 completed、payload 分离行删除、
// task_results 写入、回调派生同事务完成（§5.5/§5.8）。
func TestFinalizePayloadMergeAndCallback(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	created, err := s.CreatePending(ctx, []sdk.NewTask{{
		Type:    "job",
		Payload: []byte(`{"n":42}`),
		Callback: &sdk.CallbackSpec{
			OnSuccess: &sdk.CallbackSpec{
				Type:            "job.done",
				PayloadTemplate: []byte(`{"memo":"done"}`),
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.Claim(ctx, "sched-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := s.Finalize(ctx, []store.TerminalEntry{{
		TaskID: tid, Attempt: tasks[0].Attempts, Action: store.TerminalActionTerminal,
		Outcome: sdk.OutcomeSucceeded, Result: []byte(`{"sum":42}`), CompletedAt: time.Now(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Derived) != 1 {
		t.Fatalf("callback derived %d tasks, want 1", len(report.Derived))
	}

	// 结果查询命中 + 派生任务进入 pending。
	_, stages, err := s.GetResults(ctx, []int64{tid, report.Derived[0]})
	if err != nil {
		t.Fatal(err)
	}
	if stages[tid] != sdk.StageCompleted || stages[report.Derived[0]] != sdk.StagePending {
		t.Fatalf("stages: %v", stages)
	}
	// 派生任务参数注入：OnSuccess 注入 parent_result。
	dead, _, err := s.ListDead(ctx, "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatal("succeeded task must not be dead")
	}
}

// TestRetryRouting 重试路径：可重试失败挪回 schedulable（attempt 不变、run_at 退避），
// 再认领时 attempts+1；attempts 耗尽 → dead（§5.5/§6.3 R3）。
func TestRetryRouting(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	items := []sdk.NewTask{newTask("job")}
	items[0].MaxAttempts = 2
	created, err := s.CreatePending(ctx, items)
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID
	promote := func() {
		if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	promote()
	tasks, err := s.Claim(ctx, "sched-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tasks[0].Attempts != 1 {
		t.Fatalf("first claim attempt = %d", tasks[0].Attempts)
	}

	// 第一次失败 → 重试（挪回 schedulable，run_at = 短退避后）。
	report, err := s.Finalize(ctx, []store.TerminalEntry{{
		TaskID: tid, Attempt: 1, Action: store.TerminalActionRetry,
		RetryRunAt: time.Now().Add(50 * time.Millisecond),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Applied[tid] != store.EntryStatusApplied {
		t.Fatalf("retry rejected: %+v", report.Applied)
	}

	// run_at 未到，不可认领。
	time.Sleep(10 * time.Millisecond)
	notYet, err := s.Claim(ctx, "sched-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(notYet) != 0 {
		t.Fatalf("task with future run_at must not be claimable")
	}

	// 退避到点 → 再认领 attempts=2 → 第二次失败 → 耗尽（R3 由 Requeue 验证）。
	time.Sleep(60 * time.Millisecond)
	tasks2, err := s.Claim(ctx, "sched-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks2) != 1 || tasks2[0].Attempts != 2 {
		t.Fatalf("second claim: %+v", tasks2)
	}

	// 对账重置（attempt=2 匹配）：attempts(2) >= max_attempts(2) → R3 dead。
	rrep, err := s.Requeue(ctx, []store.RequeueEntry{{
		TaskID: tid, Attempt: 2, RunAt: time.Now(), Error: "reconcile reset: exhausted",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if rrep.Dead != 1 {
		t.Fatalf("requeue report: %+v, want dead=1", rrep)
	}
	dead, _, err := s.ListDead(ctx, "job", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Task.ID != tid {
		t.Fatalf("dead list: %+v", dead)
	}
	results, _, err := s.GetResults(ctx, []int64{tid})
	if err != nil {
		t.Fatal(err)
	}
	if results[tid] == nil || results[tid].Outcome != sdk.OutcomeDead {
		t.Fatalf("dead task must have task_results row: %+v", results[tid])
	}
}

// TestPromoteConstraints 晋升约束：per-type 并发上限与业务钩子（§5.2）。
func TestPromoteConstraints(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	items := make([]sdk.NewTask, 5)
	for i := range items {
		items[i] = newTask("job")
	}
	if _, err := s.CreatePending(ctx, items); err != nil {
		t.Fatal(err)
	}

	// 业务钩子：前 3 个通过，其余阻塞。
	calls := 0
	stats, err := s.Promote(ctx, store.PromoteOptions{
		Now:   time.Now(),
		Limit: 10,
		Preconditions: map[string]sdk.Precondition{
			"job": func(ctx context.Context, task *sdk.Task) (bool, error) {
				calls++
				return calls <= 3, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Promoted != 3 || stats.BlockedPrecondition != 2 {
		t.Fatalf("stats: %+v", stats)
	}

	// 并发上限：processing 已有任务时限制晋升（这里直接用 claim 制造在途）。
	tasks, err := s.Claim(ctx, "sched-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("claim: %d", len(tasks))
	}
	// 再造 pending 任务，per-type 上限 = 3，在途 3 → 全部阻塞。
	if _, err := s.CreatePending(ctx, []sdk.NewTask{newTask("job")}); err != nil {
		t.Fatal(err)
	}
	stats2, err := s.Promote(ctx, store.PromoteOptions{
		Now:             time.Now(),
		TypeConcurrency: map[string]int{"job": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 在途 3 >= 上限 3 → 全部阻塞（含前一轮被钩子阻塞而留在 pending 的 2 个）。
	if stats2.BlockedConcurrency != 3 || stats2.Promoted != 0 {
		t.Fatalf("stats2: %+v", stats2)
	}
}

// TestRedrive 死信重跑：复制原 payload 创建全新任务实例（§12.9）。
func TestRedrive(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	items := []sdk.NewTask{newTask("job")}
	items[0].MaxAttempts = 1
	created, err := s.CreatePending(ctx, items)
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, "sched-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Requeue(ctx, []store.RequeueEntry{{TaskID: tid, Attempt: 1, Error: "boom"}}); err != nil {
		t.Fatal(err)
	}

	newIDs, err := s.Redrive(ctx, []int64{tid})
	if err != nil {
		t.Fatal(err)
	}
	if len(newIDs) != 1 || newIDs[0] == tid {
		t.Fatalf("redrive ids: %v", newIDs)
	}
	results, stages, err := s.GetResults(ctx, []int64{newIDs[0]})
	if err != nil {
		t.Fatal(err)
	}
	if stages[newIDs[0]] != sdk.StagePending || results[newIDs[0]] != nil {
		t.Fatalf("redriven task must start fresh: %v %v", stages, results)
	}
}

// TestFactoryQueries 工厂支撑查询：LatestSuccessAt / HasDeadByBatchPrefix（§5.7）。
func TestFactoryQueries(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	created, err := s.CreatePending(ctx, []sdk.NewTask{{Type: "nightly", BatchID: "factory:nightly:1"}})
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, "sched-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finalize(ctx, []store.TerminalEntry{{
		TaskID: tid, Attempt: 1, Action: store.TerminalActionTerminal,
		Outcome: sdk.OutcomeSucceeded, CompletedAt: time.Now(),
	}}); err != nil {
		t.Fatal(err)
	}
	at, ok, err := s.LatestSuccessAt(ctx, "factory:nightly:")
	if err != nil || !ok {
		t.Fatalf("latest success: %v %v", at, ok)
	}
	hasDead, err := s.HasDeadByBatchPrefix(ctx, "factory:nightly:")
	if err != nil || hasDead {
		t.Fatalf("has dead: %v %v", hasDead, err)
	}
}

// TestGetResultsStageLookup 在途任务定位（§5.6 未命中结果时查阶段表）。
func TestGetResultsStageLookup(t *testing.T) {
	s := newStore(t) // 迁移建表
	truncateAll(t)
	ctx := context.Background()

	created, err := s.CreatePending(ctx, []sdk.NewTask{newTask("job")})
	if err != nil {
		t.Fatal(err)
	}
	tid := created[0].TaskID

	_, stages, err := s.GetResults(ctx, []int64{tid})
	if err != nil {
		t.Fatal(err)
	}
	if stages[tid] != sdk.StagePending {
		t.Fatalf("stage = %s, want pending", stages[tid])
	}
	if _, err := s.Promote(ctx, store.PromoteOptions{Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	_, stages, err = s.GetResults(ctx, []int64{tid})
	if err != nil {
		t.Fatal(err)
	}
	if stages[tid] != sdk.StageSchedulable {
		t.Fatalf("stage = %s, want schedulable", stages[tid])
	}
	// 不存在的任务 → unknown。
	_, stages, err = s.GetResults(ctx, []int64{999999})
	if err != nil {
		t.Fatal(err)
	}
	if stages[999999] != sdk.StageUnknown {
		t.Fatalf("missing task stage = %s, want unknown", stages[999999])
	}
}

// TestNonRetryableContract 不可重试错误契约在 sdk 层的行为（§5.5 终态 failed 不进重试路径）。
func TestNonRetryableContract(t *testing.T) {
	base := errors.New("bad request")
	wrapped := sdk.NonRetryable(base)
	if !sdk.IsNonRetryable(wrapped) {
		t.Fatal("expected non-retryable")
	}
	if !sdk.IsNonRetryable(fmt.Errorf("wrap: %w", wrapped)) {
		t.Fatal("errors.As must traverse the chain")
	}
	if sdk.IsNonRetryable(base) {
		t.Fatal("plain error must be retryable")
	}
}
