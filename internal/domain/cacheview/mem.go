package cacheview

import (
	"context"
	"strings"
	"sync"
	"time"
)

// MemView 是 View 的进程内实现：用于单测与「Redis 不可用」降级演练。
// 它不提供任何持久化或跨进程可见性，装配时若选择它必须在日志中明确声明
// 「异步执行视图退化为本地」，因为此时 coherence 与在途计数都只对本进程有效。
type MemView struct {
	opts Options

	mu       sync.Mutex
	tasks    map[int64]TaskState
	dedupe   map[string]time.Time
	inflight map[string]int64
	routes   map[string]string
}

// NewMem 构造进程内视图。
func NewMem(opts Options) *MemView {
	return &MemView{
		opts:     opts.withDefaults(),
		tasks:    map[int64]TaskState{},
		dedupe:   map[string]time.Time{},
		inflight: map[string]int64{},
		routes:   map[string]string{},
	}
}

// SetTaskState 写入任务视图。
func (v *MemView) SetTaskState(_ context.Context, st TaskState) error {
	if st.UpdatedAt.IsZero() {
		st.UpdatedAt = time.Now()
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.tasks[st.TaskID] = st
	return nil
}

// GetTaskState 读取任务视图。
func (v *MemView) GetTaskState(_ context.Context, taskID int64) (TaskState, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	st, ok := v.tasks[taskID]
	return st, ok, nil
}

// DeleteTaskState 删除任务视图。
func (v *MemView) DeleteTaskState(_ context.Context, taskID int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.tasks, taskID)
	return nil
}

// ClaimDedupe 进程内去重（带 TTL）。
func (v *MemView) ClaimDedupe(_ context.Context, key string) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if exp, ok := v.dedupe[key]; ok && time.Now().Before(exp) {
		return false, nil
	}
	v.dedupe[key] = time.Now().Add(v.opts.DedupeTTL)
	return true, nil
}

// ReleaseDedupe 释放去重标记。
func (v *MemView) ReleaseDedupe(_ context.Context, key string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.dedupe, key)
	return nil
}

// IncrInflight 在途计数 +1。
func (v *MemView) IncrInflight(_ context.Context, nodeID string) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.inflight[nodeID]++
	return v.inflight[nodeID], nil
}

// DecrInflight 在途计数 -1。
func (v *MemView) DecrInflight(_ context.Context, nodeID string) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.inflight[nodeID] > 0 {
		v.inflight[nodeID]--
	}
	return v.inflight[nodeID], nil
}

// Inflight 读取在途计数。
func (v *MemView) Inflight(_ context.Context, nodeID string) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.inflight[nodeID], nil
}

// SetQueueRoute 写入队列归属。
func (v *MemView) SetQueueRoute(_ context.Context, queue, nodeID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.routes[queue] = nodeID
	return nil
}

// GetQueueRoute 读取队列归属。
func (v *MemView) GetQueueRoute(_ context.Context, queue string) (string, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	node, ok := v.routes[queue]
	return node, ok, nil
}

// QueueRoutes 返回队列路由快照副本。
func (v *MemView) QueueRoutes(_ context.Context) (map[string]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]string, len(v.routes))
	for k, val := range v.routes {
		out[k] = val
	}
	return out, nil
}

// Purge 删除前缀下全部视图（MemView 忽略前缀，清空全部）。
func (v *MemView) Purge(_ context.Context, prefix string) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if prefix != "" && !strings.HasPrefix(prefix, v.opts.Prefix) {
		return 0, nil
	}
	n := int64(len(v.tasks) + len(v.dedupe) + len(v.inflight) + len(v.routes))
	v.tasks = map[int64]TaskState{}
	v.dedupe = map[string]time.Time{}
	v.inflight = map[string]int64{}
	v.routes = map[string]string{}
	return n, nil
}
