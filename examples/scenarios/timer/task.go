package timer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"

	"github.com/improvtrace/stateflux/tasks"
)

// 本场景任务的资源名。
const (
	durableResource  = "echo_durable"
	periodicResource = "echo_periodic"
)

// durableCron 是 Durable 任务的定时表达式：@every 描述符表达亚分钟节奏，
// 走与五段 cron 相同的 CronScheduler 代码路径。
const durableCron = "@every 1s"

// node 是本场景批次生成的雪花节点（各场景使用独立节点号，避免 ID 冲突）。
var node, _ = snowflake.NewNode(4)

// echoPayload 是 echo 任务的参数。
type echoPayload struct {
	N int `json:"n"`
}

// echoCall 记录一次 handler 执行。
type echoCall struct {
	TaskID  int64
	Payload string
}

// echoTask 可观测测试任务：记录每次执行供场景断言；Durable 变体同时实现
// tasks.Factory 供定时生成批次，幂等键跨 tick 稳定，重复 tick 由 repo 的
// 幂等键去重。
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

// Generate 实现 tasks.Factory：生成 batch 行持久化 echo 任务（定时批次）。
func (e *echoTask) Generate(ctx context.Context, batch int) ([]tasks.DispatchableTask, error) {
	out := make([]tasks.DispatchableTask, 0, batch)
	for i := 0; i < batch; i++ {
		payload, err := json.Marshal(echoPayload{N: i})
		if err != nil {
			return nil, err
		}
		row := durableSpec(e).NewRow(payload) // 仅取默认值，Cron/Factory 不参与落表
		row.Id = node.Generate().Int64()
		row.IdempotencyKey = fmt.Sprintf("echo:%d", i)
		out = append(out, tasks.DispatchableTask{Task: row})
	}
	return out, nil
}

// durableSpec Durable 任务定义，cron 到点生成批次。FactoryBatch=5，单并发
// 保证执行可断言。
func durableSpec(e *echoTask) *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:     durableResource,
		Concurrency:  1,
		Handler:      e.handle,
		Timeout:      time.Minute,
		Retries:      1,
		MaxAttempts:  3,
		Durable:      true,
		Cron:         durableCron,
		FactoryBatch: 5,
		Factory:      e,
	}
}

// periodicSpec 非 Durable 定时任务：直发不过表（taskID 恒 0），固定 Payload。
func periodicSpec(e *echoTask) *tasks.TaskSpec {
	payload, _ := json.Marshal(echoPayload{N: 7})
	return &tasks.TaskSpec{
		Resource:    periodicResource,
		Concurrency: 1,
		Handler:     e.handle,
		Cron:        "@every 1s",
		Payload:     payload,
	}
}
