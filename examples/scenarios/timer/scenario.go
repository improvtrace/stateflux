// Package timer 定时触发场景：非 Durable 定时任务直发不过表（taskID 恒 0）；
// Durable 任务经 CronEvent 走 factory 落表并立即发布，重复 tick 由幂等键去重；
// 禁用任务（Enabled=false）不注册 cron 条目，场景窗口内零执行。亚分钟节奏用
// @every 描述符表达，与五段 cron 走同一 CronScheduler 代码路径。
package timer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/improvtrace/stateflux/examples"
	"github.com/improvtrace/stateflux/tasks"
)

// Test 定时直发、定时落表与禁用任务。
func Test(h *examples.H, t *testing.T) {
	periodic := newEchoTask()
	durable := newEchoTask()
	disabled := newEchoTask()
	rt := h.StartRuntime(t, []*tasks.TaskSpec{durableSpec(durable), periodicSpec(periodic), disabledSpec(disabled)})
	defer rt.Stop()

	// 非 Durable：两个周期内至少两次直发，payload 为 TaskSpec 固定参数
	wantPayload, _ := json.Marshal(echoPayload{N: 7})
	examples.WaitFor(t, "定时直发至少两次", examples.WaitDefault, func() bool {
		return len(periodic.snapshot()) >= 2
	})
	for _, c := range periodic.snapshot()[:2] {
		assert.Zero(t, c.TaskID, "直发无表行，taskID 恒 0")
		assert.JSONEq(t, string(wantPayload), c.Payload)
	}

	// Durable：tick 经 factory 落表、发布并执行
	examples.WaitFor(t, "定时批次 5 行全部执行", examples.WaitDefault, func() bool {
		return len(durable.snapshot()) == 5
	})
	assert.Equal(t, 5, h.CountRows(t, durableResource))
	// 跨多个 tick（@every 1s）：同键重复生成被去重，不再产生新执行
	time.Sleep(2500 * time.Millisecond)
	assert.Equal(t, 5, h.CountRows(t, durableResource), "重复 tick 不应新增行")
	assert.Len(t, durable.snapshot(), 5, "重复 tick 不应重复执行")

	// 禁用任务：至此场景已跨 ≥ 3 个触发周期，零执行、零落表
	assert.Empty(t, disabled.snapshot(), "Enabled=false 的任务不应被调度执行")
	assert.Zero(t, h.CountRows(t, disabledResource), "禁用任务不应有落表行")
}
