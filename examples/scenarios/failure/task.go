package failure

import (
	"fmt"
	"sync"
	"time"

	"github.com/improvtrace/stateflux/tasks"
)

// 本场景任务的资源名。
const failResource = "echo_fail"

// echoCall 记录一次 handler 执行。
type echoCall struct {
	TaskID  int64
	Payload string
}

// echoTask 恒败的可观测任务：记录重试的每一次执行，供场景断言执行次数。
// 行由场景手工落表，不实现 Factory。
type echoTask struct {
	mu    sync.Mutex
	calls []echoCall
}

// newEchoTask 构造恒败任务。
func newEchoTask() *echoTask { return &echoTask{} }

// snapshot 返回已记录执行的副本。
func (e *echoTask) snapshot() []echoCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]echoCall{}, e.calls...)
}

// handle 执行并记录后恒败，作为 TaskSpec.Handler（签名 func(string, int64) error）。
func (e *echoTask) handle(payload string, taskID int64) error {
	e.mu.Lock()
	seq := len(e.calls) + 1
	e.calls = append(e.calls, echoCall{TaskID: taskID, Payload: payload})
	e.mu.Unlock()
	return fmt.Errorf("echo: injected failure (execution %d)", seq)
}

// failSpec 恒败的 Durable 任务定义：Retries=1 → 重试 1 次后终败。非定时
// Durable 任务无 Factory，行由场景手工落表。
func failSpec(e *echoTask) *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:    failResource,
		Concurrency: 1,
		Handler:     e.handle,
		Timeout:     time.Minute,
		Retries:     1,
		Durable:     true,
	}
}
