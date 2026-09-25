package worker

import (
	"sync"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// walEntryBytes 是单条未确认结果的计费口径：整条事件的 protobuf 编码大小（而非仅业务
// 结果字段），保证高水位（条数/字节）反映真实的积压体量（§10）。
func walEntryBytes(ev *taskv1.ResultEvent) int64 { return int64(proto.Size(ev)) }

// walKey 是 WAL 条目键：task_id + attempt（attempt 是 fence，§3.1）。
type walKey struct {
	TaskID  int64
	Attempt int64
}

// WAL 是结果本地日志（§5.4）：先写 WAL、再发布 ResultEvent；发布失败或断线时重放。
//
// 按设计，WAL 是**易失的派生数据而非权威状态**：未确认结果可有意丢弃，由 R1 在 grace 后
// 重置 processing 重跑（§6.2、§14.7）。高水位（HighWater）用于暂停新订阅、继续结果发送
// （§10），不构成正确性条件。
type WAL interface {
	// Add 追加一条未确认结果。
	Add(ev *taskv1.ResultEvent)
	// Ack 确认一条结果（发布成功）。
	Ack(taskID, attempt int64)
	// Pending 返回未确认结果快照（保持插入顺序）。
	Pending() []*taskv1.ResultEvent
	// Len 返回未确认条目数。
	Len() int
	// Bytes 返回未确认结果的载荷字节数（水位指标）。
	Bytes() int64
	// HighWater 报告是否达到高水位（应暂停新订阅，§10）。
	HighWater() bool
	// Close 关闭底层资源（内存实现为 no-op）。
	Close() error
}

// DefaultWALMax 是 WAL 的缺省条目上限（§10 高水位 10k 条）。
const DefaultWALMax = 10000

// DefaultWALMaxBytes 是 WAL 的缺省字节上限（§10 高水位 256MB）。
const DefaultWALMaxBytes int64 = 256 << 20

// MemWAL 是进程内 WAL：重启即丢失，仅用于测试与无磁盘场景。
type MemWAL struct {
	mu      sync.Mutex
	max     int
	maxByte int64
	entries map[walKey]*taskv1.ResultEvent
	order   []walKey
	bytes   int64
}

// NewWAL 创建进程内 WAL；max <= 0 使用 DefaultWALMax。
func NewWAL(max int) *MemWAL {
	if max <= 0 {
		max = DefaultWALMax
	}
	return &MemWAL{max: max, maxByte: DefaultWALMaxBytes, entries: map[walKey]*taskv1.ResultEvent{}}
}

// Add 追加/覆盖一条未确认结果；超出容量时丢弃最旧条目（易失派生数据，§14.7）。
func (w *MemWAL) Add(ev *taskv1.ResultEvent) {
	if ev == nil {
		return
	}
	k := walKey{TaskID: ev.GetTaskId(), Attempt: ev.GetAttempt()}
	w.mu.Lock()
	defer w.mu.Unlock()
	if old, exists := w.entries[k]; exists {
		w.bytes -= walEntryBytes(old)
	} else {
		w.order = append(w.order, k)
	}
	w.entries[k] = ev
	w.bytes += walEntryBytes(ev)
	for len(w.order) > w.max {
		oldest := w.order[0]
		w.order = w.order[1:]
		if old, ok := w.entries[oldest]; ok {
			w.bytes -= walEntryBytes(old)
			delete(w.entries, oldest)
		}
	}
}

// Ack 确认一条结果。
func (w *MemWAL) Ack(taskID, attempt int64) {
	k := walKey{TaskID: taskID, Attempt: attempt}
	w.mu.Lock()
	defer w.mu.Unlock()
	ev, ok := w.entries[k]
	if !ok {
		return
	}
	w.bytes -= walEntryBytes(ev)
	delete(w.entries, k)
	for i, key := range w.order {
		if key == k {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
}

// Pending 返回未确认结果快照。
func (w *MemWAL) Pending() []*taskv1.ResultEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*taskv1.ResultEvent, 0, len(w.order))
	for _, k := range w.order {
		if ev, ok := w.entries[k]; ok {
			out = append(out, ev)
		}
	}
	return out
}

// Len 返回未确认条目数。
func (w *MemWAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

// Bytes 返回未确认载荷字节数。
func (w *MemWAL) Bytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes
}

// HighWater 报告是否达到条目或字节高水位。
func (w *MemWAL) HighWater() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries) >= w.max || (w.maxByte > 0 && w.bytes >= w.maxByte)
}

// Close 实现 WAL；内存实现无资源可释放。
func (w *MemWAL) Close() error { return nil }

var _ WAL = (*MemWAL)(nil)
