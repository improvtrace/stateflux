// Package scheduler 实现调度角色（§5.2/§5.3，阶段 2）：约束晋升预处理、自适应认领、
// 同步分发池、异步投递、选节点。调度侧集权（§1.2.7）：任务的路由分发（选节点/选队列/优先级）、
// 并发约束与业务约束全部留在本包；调度节点全集群唯一（外部选举指定）。
package scheduler

import (
	"sync"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
)

// ResultKind 结果来源。
type ResultKind int

const (
	// ResultSync 同步执行结果（Execute RPC 响应，调度侧归集，不经 WAL，§5.4）。
	ResultSync ResultKind = iota
	// ResultAsync 异步执行结果（Collect 拉取，执行侧 WAL 供拉）。
	ResultAsync
)

// BufferedResult 统一结果缓冲条目（§5.5：同步与异步共用一条 flush 通道 →
// 全框架只有一条 PG 终态写路径）。
type BufferedResult struct {
	Entry      *dispatchv1.ResultEntry
	Kind       ResultKind
	SourceNode string // 异步结果的来源执行节点（Ack 用）；同步结果为空
}

// Buffer 统一结果缓冲：双阈值（200 条或 1s）触发归集 flush（§5.5）。
type Buffer struct {
	mu     sync.Mutex
	items  []*BufferedResult
	high   int
	signal chan struct{}
}

// NewBuffer 构造缓冲。high 为条数水位（达到即发信号）。
func NewBuffer(high int) *Buffer {
	if high <= 0 {
		high = 200
	}
	return &Buffer{high: high, signal: make(chan struct{}, 1)}
}

// Add 追加结果；达到高水位时发 flush 信号（非阻塞，合并突发）。
func (b *Buffer) Add(results ...*BufferedResult) {
	if len(results) == 0 {
		return
	}
	b.mu.Lock()
	b.items = append(b.items, results...)
	flush := len(b.items) >= b.high
	b.mu.Unlock()
	if flush {
		select {
		case b.signal <- struct{}{}:
		default:
		}
	}
}

// Signal flush 触发信号（条数水位 / 定时双触发）。
func (b *Buffer) Signal() <-chan struct{} { return b.signal }

// Len 当前缓冲条数。
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// Drain 取出全部缓冲条目（最多 max 条）。
func (b *Buffer) Drain(max int) []*BufferedResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		return nil
	}
	if max <= 0 || max > len(b.items) {
		max = len(b.items)
	}
	out := make([]*BufferedResult, max)
	copy(out, b.items[:max])
	rest := len(b.items) - max
	if rest > 0 {
		copy(b.items, b.items[max:])
	}
	b.items = b.items[:rest]
	return out
}
