package forward

import (
	"sync"
	"time"

	"github.com/improvtrace/stateflux/tasks"
)

// echoResource 是本场景内层执行任务的资源名（外层转发任务由
// tasks/persistents 承担）。
const echoResource = "forward_echo"

// echoPayload 是 echo 任务的参数。
type echoPayload struct {
	N int `json:"n"`
}

// echoCall 记录一次 handler 执行。
type echoCall struct {
	TaskID  int64
	Payload string
}

// echoTask 可观测内层任务：记录每次执行，供场景断言转发重发后恰好执行一次。
type echoTask struct {
	mu    sync.Mutex
	calls []echoCall
}

// newEchoTask 构造可观测任务。
func newEchoTask() *echoTask { return &echoTask{} }

// snapshot 返回已记录执行的副本。
func (e *echoTask) snapshot() []echoCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]echoCall{}, e.calls...)
}

// handle 执行并记录，作为 TaskSpec.Handler（签名 func(string, int64) error）。
func (e *echoTask) handle(payload string, taskID int64) error {
	e.mu.Lock()
	e.calls = append(e.calls, echoCall{TaskID: taskID, Payload: payload})
	e.mu.Unlock()
	return nil
}

// echoSpec 内层持久化任务定义：行由转发器落表，执行成功由 runtime 包装回写。
func echoSpec(e *echoTask) *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:    echoResource,
		Enabled:     true,
		Concurrency: 1,
		Handler:     e.handle,
		Timeout:     time.Minute,
		Retries:     1,
		MaxAttempts: 3,
		Durable:     true,
	}
}
