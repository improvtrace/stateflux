package data_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/internal/domain/data"
)

// TestTaskNotifierReceivesInsert 验证 §5.1 的唤醒通道：向 task_pendings 插入一行即收到通知。
func TestTaskNotifierReceivesInsert(t *testing.T) {
	dsn := os.Getenv("STATEFLUX_TEST_DSN")
	if dsn == "" {
		t.Skip("STATEFLUX_TEST_DSN not set")
	}
	store, d := openTestStore(t)
	ctx := context.Background()

	notifier, err := data.NewTaskNotifier(dsn, "public")
	if err != nil {
		t.Fatalf("notifier: %v", err)
	}
	if notifier == nil {
		t.Skip("no notifier")
	}
	t.Cleanup(func() { _ = notifier.Close() })

	if _, err := store.Enqueue(ctx, enqueueReq(9001, "notify-1")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case <-notifier.Notifications():
	case <-time.After(5 * time.Second):
		t.Fatal("no notification received after insert")
	}
	// 触发器存在性：直接插入也应触发（幂等重放）。
	if _, err := d.DB().ExecContext(ctx,
		"INSERT INTO task_pendings (id, type, channel, created_at, updated_at) VALUES (9002, 'demo', 'default', now(), now())"); err != nil {
		t.Fatalf("direct insert: %v", err)
	}
	select {
	case <-notifier.Notifications():
	case <-time.After(5 * time.Second):
		t.Fatal("no notification received after direct insert")
	}
}
