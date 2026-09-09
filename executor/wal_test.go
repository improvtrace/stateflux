package executor

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
)

func entry(id int64, attempt int64, result string) *dispatchv1.ResultEntry {
	return &dispatchv1.ResultEntry{
		TaskId: id, Attempt: attempt, Outcome: "succeeded",
		Result: []byte(result), CompletedAtUnixMs: 100,
	}
}

func openTestWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := OpenWAL(WALConfig{Dir: dir, MaxEntries: 100, MaxBytes: 1 << 20, SegmentBytes: 1 << 16}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// TestWALAppendCollectAck 落盘 → 拉取 → Ack 水位推进（§5.4/§5.5）。
func TestWALAppendCollectAck(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	defer w.Close()

	for i := int64(1); i <= 5; i++ {
		if err := w.Append(entry(i, 1, `{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	got := w.Collect(10)
	if len(got) != 5 {
		t.Fatalf("collect %d entries", len(got))
	}
	if _, b := w.Backlog(); b == 0 {
		t.Fatal("backlog bytes must be positive")
	}
	n, err := w.Ack([]int64{1, 2, 3})
	if err != nil || n != 3 {
		t.Fatalf("ack: %d %v", n, err)
	}
	if got := w.Collect(10); len(got) != 2 {
		t.Fatalf("collect after ack: %d", len(got))
	}
	// 重复 Ack 无副作用。
	if n, _ := w.Ack([]int64{1, 2, 3}); n != 0 {
		t.Fatalf("re-ack must be no-op: %d", n)
	}
	if w.OverHighWatermark() {
		t.Fatal("must not be over watermark for 5 small entries")
	}
}

// TestWALReplay 重启重放未 Ack 条目继续供拉（§6.1 执行节点宕机·结果未归集）。
func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	for i := int64(1); i <= 4; i++ {
		if err := w.Append(entry(i, 1, `x`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Ack([]int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	// 不落盘直接 Close（Append 已 Sync，模拟崩溃）。
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := openTestWAL(t, dir)
	defer w2.Close()
	// Ack 的「水位截断」以段为单位：1、2 与 3、4 同段且该段是崩溃时的当前段，文件保留，
	// 重放会把已 Ack 的 1、2 一并供拉（at-least-once）——由 PG 终态回写幂等 + 重新 Ack 收敛。
	got := w2.Collect(10)
	if len(got) != 4 {
		t.Fatalf("replayed collect: %d, want 4 (acked-in-current-segment re-delivered)", len(got))
	}
	// 重新 Ack 后回到正确水位。
	if n, _ := w2.Ack([]int64{1, 2, 3, 4}); n != 4 {
		t.Fatalf("re-ack: %d", n)
	}
	if got := len(w2.Collect(10)); got != 0 {
		t.Fatalf("collect after re-ack: %d", got)
	}
}

// TestWALCorruption 文件损坏 → 条目作废（非权威派生状态，R1 兜底），不阻断启动（§5.4）。
func TestWALCorruption(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	for i := int64(1); i <= 3; i++ {
		if err := w.Append(entry(i, 1, `x`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// 破坏第一段文件内容（截断到头部之后）。
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segs) == 0 {
		t.Fatal("no segments")
	}
	data, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segs[0], data[:len(data)-5], 0o644); err != nil {
		t.Fatal(err)
	}
	w2 := openTestWAL(t, dir)
	defer w2.Close()
	// 尾部半条 → 该条作废，其余可恢复。
	if got := len(w2.Collect(10)); got != 2 {
		t.Fatalf("corruption recovery: %d entries, want 2", got)
	}
}

// TestWALHighWatermark 高水位反压（§6.4）。
func TestWALHighWatermark(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(WALConfig{Dir: dir, MaxEntries: 3, MaxBytes: 1 << 30, SegmentBytes: 1 << 20}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := int64(1); i <= 3; i++ {
		if err := w.Append(entry(i, 1, `x`)); err != nil {
			t.Fatal(err)
		}
	}
	if !w.OverHighWatermark() {
		t.Fatal("expected high watermark with 3 unacked entries")
	}
	if _, err := w.Ack([]int64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if w.OverHighWatermark() {
		t.Fatal("watermark must clear after ack")
	}
}

// TestWALRotation 段滚动 + 全 Acked 段文件删除（Ack 水位截断，§5.4）。
func TestWALRotation(t *testing.T) {
	dir := t.TempDir()
	// 极小段强制定期滚动。
	w, err := OpenWAL(WALConfig{Dir: dir, MaxEntries: 1000, MaxBytes: 1 << 20, SegmentBytes: 300}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := int64(1); i <= 10; i++ {
		if err := w.Append(entry(i, 1, `{"pad":"xxxxxxxxxxxxxxxxxxxxx"}`)); err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segs) < 2 {
		t.Fatalf("expected rotation, got %d segments", len(segs))
	}
	// 全部 Ack → 所有旧段删除（当前段条目保留在索引但文件保留到下轮）。
	if _, err := w.Ack([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); err != nil {
		t.Fatal(err)
	}
	// 当前段文件仍在（包含未删条目），但全部已 Ack。
	if got := len(w.Collect(100)); got != 0 {
		t.Fatalf("collect after full ack: %d", got)
	}
}
