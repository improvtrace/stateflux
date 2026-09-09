package factory

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/internal/testpg"
)

// TestFactoryPeriodGeneration next = last_success + period 现算；首次无 last_success 立即生成；
// 幂等键 = factory:{name}:{next} 防同窗口重复生成（§5.7）。
func TestFactoryPeriodGeneration(t *testing.T) {
	s := testpg.Open(t)
	testpg.Truncate(t)
	f, err := New(s, config.FactoryConfig{
		Entries: []config.FactoryEntry{{
			Name:     "nightly",
			TaskType: "job",
			Period:   100 * time.Millisecond,
		}},
		TickInterval: 50 * time.Millisecond,
	}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.Run(ctx) }()
	time.Sleep(400 * time.Millisecond)
	cancel()
	<-done

	// 无 last_success → 只生成一个首个实例（后续 tick 等待 period 到期 + 幂等键挡重复）。
	db := testpg.DB(t)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pending_tasks WHERE idempotency_key LIKE 'factory:nightly:%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("factory generated %d instances, want 1 (idempotent per window)", n)
	}
}

// TestFactoryOrphanFreeze 孤儿冻结（§5.7）：工厂实例 dead 后停止自动生成。
func TestFactoryOrphanFreeze(t *testing.T) {
	s := testpg.Open(t)
	testpg.Truncate(t)
	f, err := New(s, config.FactoryConfig{
		Entries: []config.FactoryEntry{{
			Name:     "cursed",
			TaskType: "job",
			Period:   50 * time.Millisecond,
		}},
		TickInterval: 20 * time.Millisecond,
	}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.Run(ctx) }()

	// 等第一个实例生成，然后把它推到 dead（模拟重试超限）。
	db := testpg.DB(t)
	defer db.Close()
	var taskID int64
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := db.QueryRow(`SELECT id FROM pending_tasks WHERE idempotency_key LIKE 'factory:cursed:%' LIMIT 1`).Scan(&taskID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("factory instance not generated")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 直写终态（模拟实例重试耗尽 dead：pending → completed{dead}）。
	if _, err := db.Exec(`DELETE FROM pending_tasks WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
	// batch_id 带工厂前缀（真实流程中由实例的 batch_id 原样进 completed，孤儿判定据此冻结）。
	if _, err := db.Exec(`INSERT INTO completed_tasks (id, type, priority, exec_mode, run_at, timeout_ms,
		max_attempts, attempts, owner_node, error, idempotency_key, batch_id, parent_task_id,
		created_at, updated_at, outcome, payload, completed_at)
		VALUES ($1, 'job', 'normal', 'async', now(), 60000, 3, 3, '', 'dead-by-test',
		'factory:cursed:0', 'factory:cursed:1700000000000', 0, now(), now(), 'dead',
		'null'::jsonb, now())`, taskID); err != nil {
		t.Fatal(err)
	}
	// 冻结后：不再生成新实例。
	time.Sleep(300 * time.Millisecond)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pending_tasks WHERE idempotency_key LIKE 'factory:cursed:%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("frozen factory generated %d new instances, want 0", n)
	}
	cancel()
	<-done
}
