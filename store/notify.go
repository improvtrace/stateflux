package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel 任务创建唤醒通道（§5.1）。
const NotifyChannel = "stateflux_new_task"

// NotifyWatcher 低延迟唤醒（可选优化，§5.1）：pending_tasks 语句级触发器 NOTIFY，
// 专用连接 LISTEN（与 PgBouncer transaction mode 不兼容，因此独立于 ent 连接池）。
// payload 留空（仅作唤醒信号，不承载事实）；不持久化，无监听者时通知即丢；
// 定时 tick 兜底默认开启且不可配置关闭——NOTIFY 只能作低延迟优化，不能作正确性来源。
type NotifyWatcher struct {
	dsn string

	mu     sync.Mutex
	events chan struct{}
	closed chan struct{}
}

// NewNotifyWatcher 构造监听器。
func NewNotifyWatcher(dsn string) *NotifyWatcher {
	return &NotifyWatcher{dsn: dsn, events: make(chan struct{}, 1), closed: make(chan struct{})}
}

// C 唤醒信号（非阻塞，合并突发通知；消费方读到信号后应扫表，信号不承载事实）。
func (w *NotifyWatcher) C() <-chan struct{} { return w.events }

// Run 阻塞运行：连接 → LISTEN → 循环等通知，断线自动重连（1s 退避），ctx 取消退出。
func (w *NotifyWatcher) Run(ctx context.Context) {
	defer close(w.closed)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.listenOnce(ctx); err != nil && ctx.Err() == nil {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close 等待 Run 退出（Run 由调用方通过 ctx 停止）。
func (w *NotifyWatcher) Close() {
	<-w.closed
}

func (w *NotifyWatcher) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return fmt.Errorf("notify: connect: %w", err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("notify: listen: %w", err)
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("notify: wait: %w", err)
		}
		_ = n // 通知本身不承载事实，只做唤醒信号
		select {
		case w.events <- struct{}{}:
		default:
		}
	}
}
