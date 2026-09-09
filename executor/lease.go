package executor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/queue"
)

// leaseManager inprocess 租约管理（§5.4）：注册后的任务在执行中及结果未归集窗口内
// 周期续租（成员语义 = 已认领未终态，§14.2）；Ack 后移出。
// 续约失败（归属被新尝试接管/成员已消失）时停止续租——僵尸节点不能干扰新尝试。
type leaseManager struct {
	q        *queue.Queue
	nodeID   string
	lease    time.Duration
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	running map[int64]*leaseEntry
	wg      sync.WaitGroup
}

type leaseEntry struct {
	attempt int64
	lost    bool // 续约失败（归属丢失）
	cancel  context.CancelFunc
	done    chan struct{}
}

func newLeaseManager(q *queue.Queue, nodeID string, lease, interval time.Duration, log *slog.Logger) *leaseManager {
	if interval <= 0 {
		interval = lease / 3
	}
	if log == nil {
		log = slog.Default()
	}
	return &leaseManager{
		q: q, nodeID: nodeID, lease: lease, interval: interval,
		log: log, running: make(map[int64]*leaseEntry),
	}
}

// Start 为已注册任务启动周期续租。
func (m *leaseManager) Start(taskID, attempt int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.running[taskID]; exists {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &leaseEntry{attempt: attempt, cancel: cancel, done: make(chan struct{})}
	m.running[taskID] = e
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(e.done)
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := m.q.Renew(ctx, taskID, attempt, m.nodeID, m.lease)
				if err != nil {
					m.log.WarnContext(ctx, "executor: renew lease failed",
						"task_id", taskID, "err", err)
					continue
				}
				if !ok {
					// 归属校验失败：新尝试已接管或成员已消失——停止续租（僵尸防护，§3.2）。
					m.mu.Lock()
					if cur, exists := m.running[taskID]; exists && cur == e {
						cur.lost = true
					}
					m.mu.Unlock()
					m.log.WarnContext(ctx, "executor: lease ownership lost, stop renewing",
						"task_id", taskID, "attempt", attempt)
					return
				}
			}
		}
	}()
}

// Stop 停止续租；remove = true 时以 node_id + attempt 归属校验移出 inprocess（Ack 后，§5.5）。
// 返回移除是否生效。
func (m *leaseManager) Stop(taskID int64, remove bool) bool {
	m.mu.Lock()
	e, ok := m.running[taskID]
	if ok {
		delete(m.running, taskID)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	e.cancel()
	<-e.done
	if !remove || e.lost {
		return false
	}
	okRemove, err := m.q.Remove(context.Background(), taskID, e.attempt, m.nodeID)
	if err != nil {
		m.log.Warn("executor: remove from inprocess failed", "task_id", taskID, "err", err)
		return false
	}
	return okRemove
}

// StopAll 停止全部续租（优雅退出；不主动移除——成员语义交给对账收敛）。
func (m *leaseManager) StopAll() {
	m.mu.Lock()
	entries := make([]*leaseEntry, 0, len(m.running))
	for _, e := range m.running {
		entries = append(entries, e)
	}
	m.running = make(map[int64]*leaseEntry)
	m.mu.Unlock()
	for _, e := range entries {
		e.cancel()
	}
	m.wg.Wait()
}
