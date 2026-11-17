package collector

import (
	"context"
	"errors"
	"testing"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
	"github.com/improvtrace/stateflux/internal/domain/repository"
	"github.com/improvtrace/stateflux/internal/domain/schema"
)

type fakeLedger struct {
	processing *repository.Processing
	getErr     error

	completed    []repository.CompleteRequest
	completeErr  error
	requeued     []int64
	deadLettered []int64
}

func (f *fakeLedger) Complete(_ context.Context, req repository.CompleteRequest) (bool, error) {
	if f.completeErr != nil {
		return false, f.completeErr
	}
	f.completed = append(f.completed, req)
	return true, nil
}

func (f *fakeLedger) GetProcessing(context.Context, int64) (*repository.Processing, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.processing, nil
}

func (f *fakeLedger) Requeue(_ context.Context, taskID int64) (bool, error) {
	f.requeued = append(f.requeued, taskID)
	return true, nil
}

func (f *fakeLedger) DeadLetter(_ context.Context, taskID int64) (bool, error) {
	f.deadLettered = append(f.deadLettered, taskID)
	return true, nil
}

func newCollector(ledger Ledger) *Collector { return New(Options{Ledger: ledger}) }

func TestConsumeCompletesSuccess(t *testing.T) {
	ledger := &fakeLedger{}
	c := newCollector(ledger)

	if err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 2, Outcome: taskv1.Outcome_OUTCOME_SUCCEEDED, Result: []byte(`"ok"`),
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(ledger.completed) != 1 {
		t.Fatalf("complete calls = %d", len(ledger.completed))
	}
	req := ledger.completed[0]
	if req.TaskID != 1 || req.Attempt != 2 || req.Outcome != schema.OutcomeSucceeded {
		t.Fatalf("complete request = %+v", req)
	}
	if len(ledger.requeued)+len(ledger.deadLettered) != 0 {
		t.Fatal("success must not requeue or dead-letter")
	}
}

func TestConsumeRetriesFailedWithinBudget(t *testing.T) {
	ledger := &fakeLedger{processing: &repository.Processing{TaskID: 1, Attempt: 1, MaxAttempts: 3}}
	c := newCollector(ledger)

	if err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 1, Outcome: taskv1.Outcome_OUTCOME_FAILED, Error: "boom",
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(ledger.requeued) != 1 || ledger.requeued[0] != 1 {
		t.Fatalf("requeued = %v", ledger.requeued)
	}
	if len(ledger.completed) != 0 || len(ledger.deadLettered) != 0 {
		t.Fatal("retry path must not complete or dead-letter")
	}
}

func TestConsumeDeadLettersExhaustedAttempts(t *testing.T) {
	ledger := &fakeLedger{processing: &repository.Processing{TaskID: 1, Attempt: 3, MaxAttempts: 3}}
	c := newCollector(ledger)

	if err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 3, Outcome: taskv1.Outcome_OUTCOME_FAILED,
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(ledger.deadLettered) != 1 || ledger.deadLettered[0] != 1 {
		t.Fatalf("dead lettered = %v", ledger.deadLettered)
	}
	if len(ledger.requeued) != 0 || len(ledger.completed) != 0 {
		t.Fatal("exhausted task must go straight to dead letter")
	}
}

func TestConsumeRejectsStaleAttempt(t *testing.T) {
	ledger := &fakeLedger{processing: &repository.Processing{TaskID: 1, Attempt: 3, MaxAttempts: 5}}
	c := newCollector(ledger)

	// 结果携带 attempt=2：attempt fence 拒绝，无副作用。
	if err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 2, Outcome: taskv1.Outcome_OUTCOME_FAILED,
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(ledger.requeued)+len(ledger.deadLettered)+len(ledger.completed) != 0 {
		t.Fatal("stale result must have no side effects")
	}
}

func TestConsumeMissingProcessingFallsBackToComplete(t *testing.T) {
	// 在途行已被 R1 重置：GetProcessing 返回 ErrNotFound，交由 Complete 走 stale 路径裁决。
	ledger := &fakeLedger{getErr: repository.ErrNotFound}
	c := newCollector(ledger)

	if err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 9, Outcome: taskv1.Outcome_OUTCOME_FAILED,
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(ledger.completed) != 1 {
		t.Fatal("missing processing row must defer to Complete")
	}
}

// PG 瞬时故障（非 ErrNotFound）不得当作「行不存在」走 Complete——那会绕过
// 重试/死信判定；必须上抛，由调用方 NACK/保留 WAL 重放。
func TestConsumeGetProcessingErrorPropagates(t *testing.T) {
	ledger := &fakeLedger{getErr: errors.New("connection refused")}
	c := newCollector(ledger)

	err := c.Consume(context.Background(), &taskv1.ResultEvent{
		TaskId: 1, Attempt: 1, Outcome: taskv1.Outcome_OUTCOME_FAILED,
	})
	if err == nil {
		t.Fatal("GetProcessing transient failure must propagate, got nil")
	}
	if len(ledger.completed)+len(ledger.requeued)+len(ledger.deadLettered) != 0 {
		t.Fatal("transient failure must have no ledger side effects")
	}
}

func TestConsumeValidatesEvent(t *testing.T) {
	c := newCollector(&fakeLedger{})
	if err := c.Consume(context.Background(), nil); err == nil {
		t.Fatal("nil event must be rejected")
	}
	if err := c.Consume(context.Background(), &taskv1.ResultEvent{}); err == nil {
		t.Fatal("event without task id must be rejected")
	}
}
