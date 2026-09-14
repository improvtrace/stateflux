package data

import (
	"errors"
	"sync"
	"time"

	"github.com/lib/pq"
)

// connectTimeout 是 LISTEN 建立连接的等待上限。
const connectTimeout = 5 * time.Second

// NotifyChannel 是调度唤醒的 PG 通知通道名（与迁移 000003 的 pg_notify 一致，§5.1）。
const NotifyChannel = "stateflux_tasks"

// TaskNotifier 把 PG LISTEN/NOTIFY 适配为 Go 唤醒信号（§5.1）：task_pendings 插入触发
// 通知，调度器据此立即跑一轮晋升；通知只是优化，丢失由 tick 兜底。
//
// 连接由 lib/pq 的 Listener 维护：断线自动重连并重新 LISTEN；信号通道容量为 1 且合并，
// 高并发插入不会阻塞数据库端。
type TaskNotifier struct {
	listener *pq.Listener
	events   chan struct{}
	done     chan struct{}

	closeOnce sync.Once
}

// NewTaskNotifier 建立监听；DSN 为空时返回 (nil, nil)，由调用方按「无通知」处理。
func NewTaskNotifier(dsn, searchPath string) (*TaskNotifier, error) {
	dsn = WithSearchPath(dsn, defaultSearchPath(searchPath))
	if dsn == "" {
		return nil, nil
	}
	connected := make(chan struct{})
	var once sync.Once
	l := pq.NewListener(dsn, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, _ error) {
		if ev == pq.ListenerEventConnected {
			once.Do(func() { close(connected) })
		}
	})
	// 建立连接后再 LISTEN：pq 的 Listener 异步连接，未连接时 LISTEN 不生效，
	// 而 NOTIFY 不会补偿已经错过的监听者（§5.1 的唤醒不能依赖重放）。
	select {
	case <-connected:
	case <-time.After(connectTimeout):
		_ = l.Close()
		return nil, errors.New("data: notify listener connect timeout")
	}
	if err := l.Listen(NotifyChannel); err != nil {
		_ = l.Close()
		return nil, err
	}
	n := &TaskNotifier{
		listener: l,
		events:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go n.loop()
	return n, nil
}

// Notifications 返回唤醒信号通道（接收方只需知道「有新任务」）。
func (n *TaskNotifier) Notifications() <-chan struct{} {
	if n == nil {
		return nil
	}
	return n.events
}

func (n *TaskNotifier) loop() {
	defer close(n.done)
	keepalive := time.NewTicker(30 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case _, ok := <-n.listener.Notify:
			if !ok {
				return
			}
			// 合并信号：已有待处理唤醒时不再入队。
			select {
			case n.events <- struct{}{}:
			default:
			}
		case <-keepalive.C:
			// 保活探测，触发 listener 的重连逻辑。
			go func() { _ = n.listener.Ping() }()
		}
	}
}

// Close 停止监听（幂等）。
func (n *TaskNotifier) Close() error {
	if n == nil {
		return nil
	}
	n.closeOnce.Do(func() {
		_ = n.listener.Close()
	})
	return nil
}
