package worker

import (
	"sync"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// walKey 是 WAL 条目键：task_id + attempt（attempt 是 fence，§3.1）。
type walKey struct {
	TaskID  int64
	Attempt int64
}

// WAL 是结果本地日志（§5.4）：先写 WAL、再发布 ResultEvent；发布失败或断线时重放。
//
// 本实现是**进程内** WAL：重启即丢失。按设计，WAL 是易失的派生数据而非权威状态——
// 丢失只会让结果晚到，由 R1 在 grace 后重置 processing 重跑（§6.2）。磁盘 WAL 与
// 高水位反压是 §14.7 的遗留项，接口保持不变以便后续替换。
type WAL struct {
	mu      sync.Mutex
	max     int
	entries map[walKey]*taskv1.ResultEvent
	order   []walKey
}

// NewWAL 创建带容量上限的 WAL；max <= 0 使用 DefaultWALMax。
func NewWAL(max int) *WAL {
	if max <= 0 {
		max = DefaultWALMax
	}
	return &WAL{max: max, entries: map[walKey]*taskv1.ResultEvent{}}
}

// DefaultWALMax 是进程内 WAL 的条目上限（§10 高水位 10k 条的近似）。
const DefaultWALMax = 10000

// Add 追加/覆盖一条未确认结果；超出容量时丢弃最旧条目（易失派生数据，§14.7）。
func (w *WAL) Add(ev *taskv1.ResultEvent) {
	if ev == nil {
		return
	}
	k := walKey{TaskID: ev.GetTaskId(), Attempt: ev.GetAttempt()}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.entries[k]; !exists {
		w.order = append(w.order, k)
	}
	w.entries[k] = ev
	for len(w.order) > w.max {
		oldest := w.order[0]
		w.order = w.order[1:]
		delete(w.entries, oldest)
	}
}

// Ack 确认一条结果（发布成功）。
func (w *WAL) Ack(taskID, attempt int64) {
	k := walKey{TaskID: taskID, Attempt: attempt}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.entries[k]; !ok {
		return
	}
	delete(w.entries, k)
	for i, key := range w.order {
		if key == k {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
}

// Pending 返回未确认结果快照（保持插入顺序）。
func (w *WAL) Pending() []*taskv1.ResultEvent {
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

// Len 返回未确认条目数（水位指标）。
func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}
