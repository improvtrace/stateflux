package store_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/improvtrace/stateflux/internal/testpg"
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

// newStore 打开一个已迁移的 store 实例（每测试用例独立雪花节点，避免 ID 冲突）。
func newStore(t *testing.T) store.Store {
	t.Helper()
	return testpg.Open(t)
}

// newStoreWithNode 指定雪花节点（并发用例中每个 worker 独立连接）。
func newStoreWithNode(t *testing.T, node int64) store.Store {
	t.Helper()
	dsn, err := testpg.Start()
	if err != nil {
		t.Fatal(err)
	}
	sf, err := sdk.NewSnowflake(node)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(store.Options{DSN: dsn, Snowflake: sf})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func truncateAll(t *testing.T) {
	t.Helper()
	testpg.Truncate(t)
}

// newTask 便捷构造。
func newTask(typ string) sdk.NewTask {
	return sdk.NewTask{
		Type:    typ,
		Payload: []byte(`{"k":"v"}`),
	}
}
