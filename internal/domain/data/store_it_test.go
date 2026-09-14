package data_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// openTestStore 打开真实 PG（未设置 STATEFLUX_TEST_DSN 时跳过）。表由 ent schema 创建。
func openTestStore(t *testing.T) (repository.Store, *data.Data) {
	t.Helper()
	dsn := os.Getenv("STATEFLUX_TEST_DSN")
	if dsn == "" {
		t.Skip("STATEFLUX_TEST_DSN not set")
	}
	ctx := context.Background()
	d, closeFn, err := data.Open(ctx, config.Config{
		PG:    config.PG{DSN: dsn},
		Redis: config.Redis{Addrs: []string{"127.0.0.1:6379"}, DialTimeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(closeFn)
	if err := d.DB().Schema.Create(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}
	reset(t, ctx, d)
	return data.NewStore(d), d
}

func reset(t *testing.T, ctx context.Context, d *data.Data) {
	t.Helper()
	mustExec := func(sql string) {
		if _, err := d.DB().ExecContext(ctx, sql); err != nil {
			t.Fatalf("reset %q: %v", sql, err)
		}
	}
	for _, table := range []string{
		"task_pendings", "task_schedulables", "task_processings",
		"task_completeds", "task_payloads", "task_results", "task_identities",
	} {
		mustExec("DELETE FROM " + table)
	}
}

func enqueueReq(id int64, key string) repository.EnqueueRequest {
	return repository.EnqueueRequest{
		TaskID:         id,
		Type:           "demo",
		Operator:       "heartbeat",
		Priority:       schema.DefaultPriority,
		Channel:        "default",
		TimeoutMs:      1000,
		MaxAttempts:    3,
		IdempotencyKey: key,
		Payload:        json.RawMessage(`{"hello":"world"}`),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func TestStoreEnqueueClaimsAndCompletes(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()

	// 1) 创建 + 幂等：重复键返回原 task_id 且不新建。
	res, err := store.Enqueue(ctx, enqueueReq(1001, "it-key-1"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !res.Created || res.TaskID != 1001 {
		t.Fatalf("first enqueue: got %+v", res)
	}
	dup, err := store.Enqueue(ctx, enqueueReq(1002, "it-key-1"))
	if err != nil {
		t.Fatalf("dup enqueue: %v", err)
	}
	if dup.Created || dup.TaskID != 1001 {
		t.Fatalf("dedupe should return original id: got %+v", dup)
	}

	// 2) 晋升 pending -> schedulable。
	promoted, err := store.Promote(ctx, 10)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("promote = %d, want 1", promoted)
	}

	// 3) 认领 schedulable -> processing，attempts +1。
	claimed, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskID != 1001 {
		t.Fatalf("claim = %+v", claimed)
	}
	if claimed[0].Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", claimed[0].Attempt)
	}
	payload, err := store.GetPayload(ctx, 1001)
	if err != nil || len(payload) == 0 {
		t.Fatalf("payload = %q err = %v", payload, err)
	}

	// 4) 陈旧 attempt 被拒绝且无副作用。
	stale, err := store.Complete(ctx, repository.CompleteRequest{
		TaskID: 1001, Attempt: 99, Outcome: schema.OutcomeSucceeded,
	})
	if err != nil {
		t.Fatalf("stale complete: %v", err)
	}
	if stale {
		t.Fatal("stale attempt must be rejected")
	}

	// 5) 正确 attempt 终态化。
	ok, err := store.Complete(ctx, repository.CompleteRequest{
		TaskID: 1001, Attempt: 1, Outcome: schema.OutcomeSucceeded,
		Result: json.RawMessage(`{"ok":true}`), CompletedAt: time.Now(),
	})
	if err != nil || !ok {
		t.Fatalf("complete: ok=%v err=%v", ok, err)
	}
	got, err := store.GetResult(ctx, 1001)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if got.Outcome != schema.OutcomeSucceeded || got.Attempt != 1 {
		t.Fatalf("result = %+v", got)
	}

	// 6) 重复终态被墓碑主键拒绝。
	again, err := store.Complete(ctx, repository.CompleteRequest{
		TaskID: 1001, Attempt: 1, Outcome: schema.OutcomeSucceeded,
	})
	if err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}
	if again {
		t.Fatal("duplicate result must be rejected by tombstone")
	}
}

func TestStoreRequeueAndResetExpired(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()

	for _, id := range []int64{2001, 2002} {
		if _, err := store.Enqueue(ctx, enqueueReq(id, "")); err != nil {
			t.Fatalf("enqueue %d: %v", id, err)
		}
	}
	if _, err := store.Promote(ctx, 10); err != nil {
		t.Fatalf("promote: %v", err)
	}
	claimed, err := store.Claim(ctx, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}

	// Requeue：processing -> schedulable，attempts 不增加。
	moved, err := store.Requeue(ctx, 2001)
	if err != nil || !moved {
		t.Fatalf("requeue: moved=%v err=%v", moved, err)
	}
	reclaimed, err := store.Claim(ctx, 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim = %+v err=%v", reclaimed, err)
	}
	if reclaimed[0].TaskID != 2001 || reclaimed[0].Attempt != 2 {
		t.Fatalf("reclaim = %+v, want task 2001 attempt 2", reclaimed[0])
	}

	// R1：把 updated_at 拨回过去后 ResetExpired 应把剩余在途行移回 schedulable。
	if _, err := store.ResetExpired(ctx, time.Now().Add(time.Hour), 10); err != nil {
		t.Fatalf("reset expired: %v", err)
	}
	pend, _ := store.CountSchedulable(ctx)
	if pend < 1 {
		t.Fatalf("schedulable = %d, want >=1 after reset", pend)
	}
}

func TestStoreDeadLetter(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()
	if _, err := store.Enqueue(ctx, enqueueReq(3001, "")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Promote(ctx, 10); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := store.Claim(ctx, 10); err != nil {
		t.Fatalf("claim: %v", err)
	}
	dl, err := store.DeadLetter(ctx, 3001)
	if err != nil || !dl {
		t.Fatalf("dead letter: %v %v", dl, err)
	}
	r, err := store.GetResult(ctx, 3001)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if r.Outcome != schema.OutcomeDead {
		t.Fatalf("outcome = %v, want dead", r.Outcome)
	}
}
