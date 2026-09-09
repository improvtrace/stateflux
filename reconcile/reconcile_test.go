package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/internal/testpg"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

func TestMain(m *testing.M) {
	if _, err := testpg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	testpg.Stop()
	os.Exit(code)
}

func newReconciler(t *testing.T) (*Reconciler, store.Store, *queue.Queue, *miniredis.Miniredis) {
	t.Helper()
	s := testpg.Open(t)
	testpg.Truncate(t)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	q := queue.New(rdb)
	r := New(Options{
		Store: s, Queue: q,
		Cfg:    config.ReconcileConfig{Interval: time.Hour, Grace: 300 * time.Millisecond},
		Logger: slog.New(slog.DiscardHandler),
	})
	return r, s, q, mr
}

// seedProcessing 直插一条 processing 行（updated_at 可回拨）。
func seedProcessing(t *testing.T, s store.Store, taskID int64, attempts, maxAttempts int64, updatedAt time.Time) {
	t.Helper()
	db := testpg.DB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO pending_tasks (id, type, priority, exec_mode, run_at, timeout_ms,
		max_attempts, attempts, owner_node, error, idempotency_key, batch_id, parent_task_id,
		created_at, updated_at) VALUES ($1, 'job', 'normal', 'async', now(), 60000, $3, $2,
		'sched-1', '', '', '', 0, $4, $4)`, taskID, attempts, maxAttempts, updatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_payloads (task_id, payload, created_at)
		VALUES ($1, '{}'::jsonb, now())`, taskID); err != nil {
		t.Fatal(err)
	}
	// 挪到 processing（模拟已认领）。
	if _, err := db.Exec(`INSERT INTO processing_tasks (id, type, priority, exec_mode, run_at, timeout_ms,
		max_attempts, attempts, owner_node, error, idempotency_key, batch_id, parent_task_id,
		created_at, updated_at)
		SELECT id, type, priority, exec_mode, run_at, timeout_ms, max_attempts, attempts, owner_node,
		error, idempotency_key, batch_id, parent_task_id, created_at, updated_at
		FROM pending_tasks WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM pending_tasks WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileR1Reset R1：processing 滞留超 grace 且无 inprocess 成员 → 挪回 schedulable
// （run_at = 退避），attempts 不变（认领时再 +1，§14.3）。
func TestReconcileR1Reset(t *testing.T) {
	r, s, _, _ := newReconciler(t)
	ctx := context.Background()
	id := sdk.DefaultSnowflake().MustNext()
	seedProcessing(t, s, id, 1, 3, time.Now().Add(-2*time.Second))

	if err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	db := testpg.DB(t)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM schedulable_tasks WHERE id = $1 AND attempts = 1 AND run_at > now()`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("task not requeued to schedulable: %d", n)
	}
	// 租约内的任务不重置：注册 inprocess（未过期）后再插一条滞留任务。
	id2 := sdk.DefaultSnowflake().MustNext()
	seedProcessing(t, s, id2, 1, 3, time.Now().Add(-2*time.Second))
	if _, err := r.q.Register(ctx, id2, 1, "node-a", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM processing_tasks WHERE id = $1`, id2).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("task with live lease must not be reset")
	}
}

// TestReconcileR3Dead R3：attempts 耗尽的滞留任务 → completed{dead} + task_results（§6.3）。
func TestReconcileR3Dead(t *testing.T) {
	r, s, _, _ := newReconciler(t)
	ctx := context.Background()
	id := sdk.DefaultSnowflake().MustNext()
	seedProcessing(t, s, id, 3, 3, time.Now().Add(-2*time.Second))

	if err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	db := testpg.DB(t)
	defer db.Close()
	var outcome string
	if err := db.QueryRow(`SELECT outcome FROM completed_tasks WHERE id = $1`, id).Scan(&outcome); err != nil {
		t.Fatalf("dead row missing: %v", err)
	}
	if outcome != "dead" {
		t.Fatalf("outcome %s, want dead", outcome)
	}
	var resOutcome string
	if err := db.QueryRow(`SELECT outcome FROM task_results WHERE task_id = $1`, id).Scan(&resOutcome); err != nil {
		t.Fatalf("task_results row missing: %v", err)
	}
}

// TestReconcileR2Ghosts R2：inprocess 中 PG 已不存在的幽灵条目 → 强制移除（§6.3）。
func TestReconcileR2Ghosts(t *testing.T) {
	r, s, q, _ := newReconciler(t)
	ctx := context.Background()
	ghost := sdk.DefaultSnowflake().MustNext()
	if _, err := q.Register(ctx, ghost, 1, "node-a", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	size, err := q.InprocessSize(ctx)
	if err != nil || size != 0 {
		t.Fatalf("ghost entries remain: %d %v", size, err)
	}
	_ = s
}
