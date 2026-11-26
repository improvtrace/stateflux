// Package startup_recovery 启动恢复场景：服务重启/停止前未执行完的任务
// （pending/dispatching/dispatched 三种在途形态），在启动后的首轮对账被
// 重新 dispatch 到任务队列并执行收敛。
package startup_recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/improvtrace/stateflux/examples"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

// Test 未完成行的启动重派。
func Test(h *examples.H, t *testing.T) {
	echo := newEchoTask()
	// 重启前状态手工构造（不经 runtime）：r1 留 pending（落表后未及发布），
	// r2 认领后未发布（dispatching），r3 发布后未执行（dispatched）
	id1 := h.SeedRow(t, echoResource, []byte(`{"n":1}`), "recovery-1")
	id2 := h.SeedRow(t, echoResource, []byte(`{"n":2}`), "recovery-2")
	id3 := h.SeedRow(t, echoResource, []byte(`{"n":3}`), "recovery-3")

	claimed, err := h.Repo.ClaimPending(h.Ctx, &states.TaskPersistentClaim{
		Resources: []string{echoResource}, Batch: 2, Node: "crash-node",
	})
	assert.NoError(t, err)
	assert.Len(t, claimed, 2, "按 created_at 升序认领前两行")
	assert.Equal(t, id1, claimed[0].Id)
	assert.Equal(t, id2, claimed[1].Id)
	assert.NoError(t, h.Repo.MarkDispatched(h.Ctx, []int64{id2}))
	assert.Equal(t, states.TaskPhasePending, h.GetRow(t, id3).Phase)
	assert.Equal(t, states.TaskPhaseDispatching, h.GetRow(t, id1).Phase)
	assert.Equal(t, states.TaskPhaseDispatched, h.GetRow(t, id2).Phase)

	// 服务崩溃重启：队列内容与 backend 执行状态全丢，行留在各在途阶段
	h.FlushRedis(t)
	h.AgeRows(t)

	// 启动 runtime：首轮对账把在途行回 pending 并重派全部未完成行
	rt := h.StartRuntime(t, []*tasks.TaskSpec{echoSpec(echo)})
	defer rt.Stop()

	targets := []int64{id1, id2, id3}
	examples.WaitFor(t, "未完成行全部重派执行", examples.WaitDefault, func() bool {
		return len(echo.callsOf(targets)) == 3
	})
	assert.Len(t, echo.callsOf(targets), 3, "每行恰好执行一次")
	assert.Equal(t, 3, h.CountTerminalByIds(t, targets, states.TaskOutcomeSucceeded))
}
