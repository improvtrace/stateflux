package data_test

import (
	"context"
	"testing"

	"github.com/improvtrace/stateflux/internal/domain"
	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/internal/domain/data/ent/taskpending"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

// nestedCallback 构造 depth 层嵌套的 on_success 回调（每层派生任务自带下一层回调）。
func nestedCallback(depth int) *domain.CallbackSpec {
	var cb *domain.CallbackSpec
	for i := 0; i < depth; i++ {
		cb = &domain.CallbackSpec{
			OnSuccess: &domain.CallbackTask{Type: "chain", Operator: "ok", Channel: "default", Callback: cb},
		}
	}
	return cb
}

func countCompleteds(t *testing.T, d *data.Data) int {
	t.Helper()
	n, err := d.DB().TaskCompleted.Query().Count(context.Background())
	if err != nil {
		t.Fatalf("count completed: %v", err)
	}
	return n
}

func countPendings(t *testing.T, d *data.Data) int {
	t.Helper()
	n, err := d.DB().TaskPending.Query().Count(context.Background())
	if err != nil {
		t.Fatalf("count pending: %v", err)
	}
	return n
}

func TestCallbackDerivationSingle(t *testing.T) {
	store, d := openTestStore(t)
	ctx := context.Background()

	raw, err := domain.MarshalCallback(nestedCallback(1))
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	req := enqueueReq(4001, "cb-single")
	req.Callback = raw
	if _, err := store.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Promote(ctx, 10); err != nil {
		t.Fatalf("promote: %v", err)
	}
	claimed, err := store.Claim(ctx, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}
	ok, err := store.Complete(ctx, repository.CompleteRequest{
		TaskID: 4001, Attempt: claimed[0].Attempt, Outcome: schema.OutcomeSucceeded,
	})
	if err != nil || !ok {
		t.Fatalf("complete: ok=%v err=%v", ok, err)
	}
	// 终态事务内派生：pending 出现一条 chain 任务，父为 4001，幂等键 parent:attempt:outcome。
	derived, err := d.DB().TaskPending.Query().Where(taskpending.ParentTaskID(4001)).Only(ctx)
	if err != nil {
		t.Fatalf("query derived: %v", err)
	}
	if derived.ParentTaskID != 4001 || derived.IdempotencyKey != "4001:1:1" || derived.Type != "chain" {
		t.Fatalf("derived = parent=%d key=%q type=%q", derived.ParentTaskID, derived.IdempotencyKey, derived.Type)
	}
}

func TestCallbackDerivationDepthLimit(t *testing.T) {
	store, d := openTestStore(t)
	ctx := context.Background()

	raw, err := domain.MarshalCallback(nestedCallback(domain.MaxCallbackDepth + 4))
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	req := enqueueReq(5001, "cb-chain")
	req.Callback = raw
	if _, err := store.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 反复晋升/认领/终态，链条应在深度上限处停止派生。
	for round := 0; round < domain.MaxCallbackDepth+6; round++ {
		promoted, err := store.Promote(ctx, 10)
		if err != nil {
			t.Fatalf("promote: %v", err)
		}
		if promoted == 0 {
			break
		}
		claimed, err := store.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		for _, p := range claimed {
			if _, err := store.Complete(ctx, repository.CompleteRequest{
				TaskID: p.TaskID, Attempt: p.Attempt, Outcome: schema.OutcomeSucceeded,
			}); err != nil {
				t.Fatalf("complete %d: %v", p.TaskID, err)
			}
		}
	}

	completed := countCompleteds(t, d)
	if want := domain.MaxCallbackDepth + 1; completed != want {
		t.Fatalf("completed = %d, want %d (depth limit)", completed, want)
	}
	if pending := countPendings(t, d); pending != 0 {
		t.Fatalf("pending = %d, want 0 (no derivation past depth limit)", pending)
	}
}
