package startup_recovery

import (
	"sync"
	"time"

	"github.com/improvtrace/stateflux/tasks"
)

// 本场景任务的资源名。
const echoResource = "echo_durable"

// echoCall 记录一次 handler 执行。
type echoCall struct {
	TaskID  int64
	Payload string
}

// echoTask 可观测测试任务：记录每次执行供场景断言重派结果。行由场景手工
// 落表，无 Factory（不经定时触发，重派全靠启动恢复通道）。
type echoTask struct {
	mu    sync.Mutex
	calls []echoCall
}

// newEchoTask 构造可观测任务。
func newEchoTask() *echoTask { return &echoTask{} }

// callsOf 返回目标任务的已记录执行 ID 序列。
func (e *echoTask) callsOf(ids []int64) []int64 {
	want := make(map[int64]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []int64
	for _, c := range e.calls {
		if want[c.TaskID] {
			out = append(out, c.TaskID)
		}
	}
	return out
}

// handle 执行并记录，作为 TaskSpec.Handler（签名 func(string, int64) error）。
func (e *echoTask) handle(payload string, taskID int64) error {
	e.mu.Lock()
	e.calls = append(e.calls, echoCall{TaskID: taskID, Payload: payload})
	e.mu.Unlock()
	return nil
}

// echoSpec Durable 任务定义：单并发保证执行可断言。
func echoSpec(e *echoTask) *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:    echoResource,
		Concurrency: 1,
		Handler:     e.handle,
		Timeout:     time.Minute,
		Retries:     1,
		MaxAttempts: 3,
		Durable:     true,
	}
}
