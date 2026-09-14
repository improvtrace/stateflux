package worker

import (
	"path/filepath"
	"testing"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

func TestDiskWALReplayAndCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.wal")
	w, err := OpenDiskWAL(path, 10, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w.Add(&taskv1.ResultEvent{TaskId: 1, Attempt: 1, Result: []byte("a")})
	w.Add(&taskv1.ResultEvent{TaskId: 2, Attempt: 1, Result: []byte("bb")})
	w.Add(&taskv1.ResultEvent{TaskId: 3, Attempt: 1})
	w.Ack(2, 1)
	if w.Len() != 2 {
		t.Fatalf("len = %d, want 2", w.Len())
	}
	if w.Bytes() != 1 {
		t.Fatalf("bytes = %d, want 1", w.Bytes())
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重放：未确认条目应保留，已确认条目消失。
	reopened, err := OpenDiskWAL(path, 10, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.Len() != 2 {
		t.Fatalf("replayed len = %d, want 2", reopened.Len())
	}
	pending := reopened.Pending()
	if len(pending) != 2 || pending[0].GetTaskId() != 1 || pending[1].GetTaskId() != 3 {
		t.Fatalf("pending = %+v", pending)
	}
	// 压缩后再次重放仍保留未确认集合。
	if err := reopened.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	_ = reopened.Close()

	again, err := OpenDiskWAL(path, 10, 0)
	if err != nil {
		t.Fatalf("reopen after compact: %v", err)
	}
	defer again.Close()
	if again.Len() != 2 {
		t.Fatalf("after compact len = %d, want 2", again.Len())
	}
}

func TestDiskWALHighWater(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hw.wal")
	w, err := OpenDiskWAL(path, 1, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()
	if w.HighWater() {
		t.Fatal("empty WAL must not be at high water")
	}
	w.Add(&taskv1.ResultEvent{TaskId: 1, Attempt: 1})
	if !w.HighWater() {
		t.Fatal("max=1 with one entry must be at high water")
	}
	w.Ack(1, 1)
	if w.HighWater() {
		t.Fatal("after ack must drop below high water")
	}
}
