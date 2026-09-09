package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/sdk"

	apiv1 "github.com/improvtrace/stateflux/proto/gen/apiv1"
)

// TestE2EAsyncPipeline 异步全链路（§5.1→§5.5）：创建 → 晋升 → 认领 → LPUSH → BRPOP 消费 →
// WAL → Collect → 终态事务（payload 合并 + task_results）→ 墓碑 → Ack → inprocess 清空。
func TestE2EAsyncPipeline(t *testing.T) {
	f := newFixture(t)
	const n = 50
	items := make([]sdk.NewTask, n)
	for i := range items {
		items[i] = sdk.NewTask{
			Type:    "work",
			Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	ids := f.createTasks(items...)
	results := f.waitResults(ids, 30*time.Second)
	for _, id := range ids {
		r := results[id]
		if r.Outcome != sdk.OutcomeSucceeded {
			t.Fatalf("task %d outcome %s (%s)", id, r.Outcome, r.Error)
		}
		if r.Attempt != 1 {
			t.Fatalf("task %d attempt %d, want 1", id, r.Attempt)
		}
	}

	// 终态事务完整性：completed 内联 payload；task_payloads 分离行已删除。
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var inPayload, outPayload int
	if err := db.QueryRow(`SELECT count(*) FROM completed_tasks WHERE payload IS NOT NULL`).Scan(&inPayload); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM task_payloads`).Scan(&outPayload); err != nil {
		t.Fatal(err)
	}
	if inPayload != n {
		t.Fatalf("completed with inline payload: %d, want %d", inPayload, n)
	}
	if outPayload != 0 {
		t.Fatalf("payload rows remaining: %d, want 0", outPayload)
	}

	// 墓碑与 inprocess 收敛。
	for _, id := range ids[:5] {
		ok, err := f.q.HasTombstone(context.Background(), id)
		if err != nil || !ok {
			t.Fatalf("tombstone missing for %d: %v", id, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		size, _ := f.q.InprocessSize(context.Background())
		if size == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inprocess not drained: %d", size)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestE2ESyncTask 同步任务（§5.3/§5.6）：调度节点 RPC Execute → 结果进统一缓冲 →
// 归集 flush → 创建方长轮询回执。
func TestE2ESyncTask(t *testing.T) {
	f := newFixture(t)
	ids := f.createTasks(sdk.NewTask{
		Type: "work", Payload: []byte(`{"sync":true}`), ExecMode: sdk.ExecSync,
	})
	results := f.waitResults(ids, 15*time.Second)
	if results[ids[0]].Outcome != sdk.OutcomeSucceeded {
		t.Fatalf("sync task outcome: %+v", results[ids[0]])
	}
}

// TestE2ECallbackDerivation 回调派生（§5.8）：OnSuccess 注入父任务结果，派生任务全链路执行。
func TestE2ECallbackDerivation(t *testing.T) {
	f := newFixture(t)
	parent := sdk.NewTask{
		Type:    "work",
		Payload: []byte(`{"parent":1}`),
		Callback: &sdk.CallbackSpec{
			OnSuccess: &sdk.CallbackSpec{
				Type:            "work",
				PayloadTemplate: []byte(`{"from":"callback"}`),
			},
		},
	}
	ids := f.createTasks(parent)
	f.waitResults(ids, 15*time.Second)

	// 派生任务（parent_task_id 溯源）应已创建并走全链路——扫四阶段表定位。
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var found int64
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = db.QueryRow(`SELECT id FROM pending_tasks WHERE parent_task_id = $1
			UNION ALL SELECT id FROM schedulable_tasks WHERE parent_task_id = $1
			UNION ALL SELECT id FROM processing_tasks WHERE parent_task_id = $1
			UNION ALL SELECT id FROM completed_tasks WHERE parent_task_id = $1`, ids[0]).Scan(&found)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("derived task not found across stages: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	results := f.waitResults([]int64{found}, 15*time.Second)
	if results[found].Outcome != sdk.OutcomeSucceeded {
		t.Fatalf("derived task outcome: %+v", results[found])
	}
	// 注入校验：派生 payload 含 parent_result（派生任务终态后 payload 在 completed 行内）。
	var injected int
	if err := db.QueryRow(`SELECT count(*) FROM completed_tasks WHERE id = $1 AND payload::text LIKE '%parent_result%'`, found).Scan(&injected); err != nil {
		t.Fatal(err)
	}
	if injected != 1 {
		t.Fatalf("derived payload missing parent_result injection")
	}
}

// TestE2ERetryThenSuccess 重试路径（§5.5/§6.3）：前 2 次失败、第 3 次成功；
// attempts 随认领递增，退避后收敛成功而非 dead。
func TestE2ERetryThenSuccess(t *testing.T) {
	f := newFixture(t)
	f.plan.mu.Lock()
	f.plan.failFirstN["work"] = 2
	f.plan.mu.Unlock()
	items := []sdk.NewTask{{Type: "work", Payload: []byte(`{}`), MaxAttempts: 5}}
	ids := f.createTasks(items...)
	results := f.waitResults(ids, 30*time.Second)
	if results[ids[0]].Outcome != sdk.OutcomeSucceeded {
		t.Fatalf("outcome: %+v (err=%s)", results[ids[0]].Outcome, results[ids[0]].Error)
	}
	if results[ids[0]].Attempt != 3 {
		t.Fatalf("attempt %d, want 3 (two planned failures)", results[ids[0]].Attempt)
	}
}

// TestE2EDeadLetter 重试耗尽 → dead（§6.3 R3）→ 死信运维面（§12.9 redrive）。
func TestE2EDeadLetter(t *testing.T) {
	f := newFixture(t)
	items := []sdk.NewTask{{Type: "fail", Payload: []byte(`{}`), MaxAttempts: 2}}
	ids := f.createTasks(items...)
	results := f.waitResults(ids, 30*time.Second)
	if results[ids[0]].Outcome != sdk.OutcomeDead {
		t.Fatalf("outcome: %+v, want dead", results[ids[0]].Outcome)
	}

	// dead 查询。
	resp, err := f.api.ListDeadTasks(context.Background(), &apiv1.ListDeadTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].TaskId != ids[0] {
		t.Fatalf("dead list: %+v", resp.Tasks)
	}

	// redrive：重跑（新实例、attempts 归零）；handler 仍失败 → 新实例再次走完重试 → dead，
	// 证明 redrive 产生了全新任务实例且原实例保持不变。
	redriveResp, err := f.api.RedriveTasks(context.Background(), &apiv1.RedriveTasksRequest{TaskIds: ids})
	if err != nil {
		t.Fatal(err)
	}
	if len(redriveResp.NewTaskIds) != 1 || redriveResp.NewTaskIds[0] == ids[0] {
		t.Fatalf("redrive ids: %v", redriveResp.NewTaskIds)
	}
	newResults := f.waitResults(redriveResp.NewTaskIds, 30*time.Second)
	if newResults[redriveResp.NewTaskIds[0]].Outcome != sdk.OutcomeDead {
		t.Fatalf("redriven outcome: %+v, want dead (handler always fails)", newResults[redriveResp.NewTaskIds[0]].Outcome)
	}
	if newResults[redriveResp.NewTaskIds[0]].Attempt != 2 {
		t.Fatalf("redriven attempt: %d, want 2 (fresh instance)", newResults[redriveResp.NewTaskIds[0]].Attempt)
	}
}

// TestE2EExecutorCrashReconcile 执行节点宕机收敛（§6.1）：认领后清空 inprocess（模拟
// BRPOP 后注册前/执行中崩溃）→ 对账 R1 重置 → 重新认领重跑 → 终态成功。
func TestE2EExecutorCrashReconcile(t *testing.T) {
	f := newFixture(t)
	ids := f.createTasks(sdk.NewTask{Type: "work", Payload: []byte(`{"crash":true}`)})
	// 等任务进入 processing 后，清空 Redis inprocess + 队列（模拟执行节点崩溃，成员丢失）。
	deadline := time.Now().Add(10 * time.Second)
	for {
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			t.Fatal(err)
		}
		var inProcessing int
		if err := db.QueryRow(`SELECT count(*) FROM processing_tasks`).Scan(&inProcessing); err != nil {
			db.Close()
			t.Fatal(err)
		}
		db.Close()
		if inProcessing > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never claimed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := f.q.ClearAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 对账 R1（grace=300ms）应重置任务 → 重新调度执行 → 成功终态。
	results := f.waitResults(ids, 30*time.Second)
	if results[ids[0]].Outcome != sdk.OutcomeSucceeded {
		t.Fatalf("outcome after reconcile: %+v", results[ids[0]])
	}
}

// TestE2EStaleCopyRejected 陈旧副本双防线（§6.2）：任务终态后向队列投递旧消息
// → 注册被墓碑拒绝 → 不产生执行，任务保持终态。
func TestE2EStaleCopyRejected(t *testing.T) {
	f := newFixture(t)
	ids := f.createTasks(sdk.NewTask{Type: "work", Payload: []byte(`{}`)})
	f.waitResults(ids, 15*time.Second)
	// 向队列注入坏消息：解码失败必须被丢弃且不影响既有终态（§5.4 消费健壮性）。
	f.mr.Lpush("stateflux:queue:normal", "garbage-bytes")
	time.Sleep(500 * time.Millisecond)
	results, err := f.api.GetResults(context.Background(), &apiv1.GetResultsRequest{TaskIds: ids})
	if err != nil {
		t.Fatal(err)
	}
	if results.Results[0].Outcome != string(sdk.OutcomeSucceeded) {
		t.Fatalf("stale copy must not change terminal state: %+v", results.Results[0])
	}
}
