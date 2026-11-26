// Package failure 失败重试与终态收敛场景：执行内重试（Retries）耗尽后仍失败，
// 行保持 dispatched（失败不内联回写），由对账按 backend FAILURE 收敛为 failed。
package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/improvtrace/stateflux/examples"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

// Test 重试耗尽与对账收敛。
func Test(h *examples.H, t *testing.T) {
	e := newEchoTask()
	rt := h.StartRuntime(t, []*tasks.TaskSpec{failSpec(e)})
	defer rt.Stop()

	id := h.SeedRow(t, failResource, []byte(`{"n":1}`), "echo-fail-1")

	// Retries=1 → 共执行 2 次后终败
	examples.WaitFor(t, "重试耗尽（共执行 2 次）", examples.WaitRetry, func() bool {
		return len(e.snapshot()) == 2
	})
	row := h.GetRow(t, id)
	assert.Equal(t, states.TaskPhaseDispatched, row.Phase, "失败不内联回写，保持 dispatched")
	assert.Equal(t, states.TaskOutcomeUnset, row.Outcome)

	// 回拨 updated_at（越过 30s 对账下限），对账按 backend FAILURE 收敛
	h.AgeRows(t)
	examples.WaitFor(t, "对账收敛为 failed", examples.WaitDefault, func() bool {
		return h.CountTerminal(t, failResource, states.TaskOutcomeFailed) == 1
	})
	row = h.GetRow(t, id)
	assert.Equal(t, states.TaskOutcomeFailed, row.Outcome)
	assert.NotEmpty(t, row.ErrorSummary)
	assert.NotNil(t, row.FinishedAt)
}
