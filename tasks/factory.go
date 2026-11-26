package tasks

import (
	"context"

	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
)

// DispatchableTask 是 Factory.Generate 的产物，持久化与直发二选一：
//   - Task 非空：持久化任务，调用方先落 task_persistents（dispatch 前持久化，
//     之后按 phase 维护各阶段状态、支持重启恢复），再从表行组装 Signature 发布；
//   - Task 为空：以 Sig 直接发布（无状态直发，丢了由下轮周期自然覆盖）。
type DispatchableTask struct {
	Task *states.TaskPersistent
	Sig  *queue.Signature
}

// Factory 在 cron 到点时被 CronScheduler 调用，批量生成待发布任务。
// 只负责生成，永不直接发布到队列——落表与发布由调用方（publisher）完成。
type Factory interface {
	Generate(ctx context.Context, batch int) ([]DispatchableTask, error)
}
