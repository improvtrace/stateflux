// Package smoke 冒烟场景：走 schedule.Run 真实入口（默认注册表 = 持久化转发
// 消费侧），验证完整生命周期——落表 → 启动恢复重派 → 两阶段发布 → 分节点
// 路由 → 转发任务执行 → 成功内联回写 → 停机。
//
// 框架不再内置业务任务，场景以「待转发的持久化签名行」驱动入口：入口把这些
// 行发布给 persistent_forward，转发器落内层行并重发。内层签名故意携带无效
// 参数（payload {}），使内层任务快速失败终态，不影响被断言的外层转发行。
package smoke

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/examples"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks/persistents"
)

// node 生成本场景内层签名的任务 ID（各场景使用独立节点号，避免 ID 冲突）。
var node, _ = snowflake.NewNode(6)

// forwardPayload 构造一行转发参数：内层签名指向 persistents 资源本身，但
// payload 为 {}——转发器重发后内层执行因解不出内层签名而失败终态，外层转发
// 行仍按协议成功收敛。
func forwardPayload(t *testing.T, id int64) json.RawMessage {
	t.Helper()
	inner, err := json.Marshal(&queue.Signature{
		UUID: states.SignatureUUID(id),
		Name: persistents.TaskName,
		Args: []queue.Arg{
			{Type: "string", Value: "{}"},
			{Type: "int64", Value: id},
		},
		Headers: queue.Headers{dispatch.AttributeResource: persistents.Resource},
	})
	require.NoError(t, err)
	payload, err := json.Marshal(dispatch.PersistentForwardPayload{Signature: string(inner)})
	require.NoError(t, err)
	return payload
}

// Test Run 入口生命周期与停机语义。
func Test(h *examples.H, t *testing.T) {
	var ids []int64
	for i := 1; i <= 3; i++ {
		ids = append(ids, h.SeedRow(t, persistents.Resource, forwardPayload(t, node.Generate().Int64()), ""))
	}

	errCh, cancel := h.StartEntry(t) // 节点 ID 由入口经 dm 桩获取（node_id 即 examples.NodeName）

	examples.WaitFor(t, "3 行转发任务全部成功收敛", examples.WaitDefault, func() bool {
		return h.CountTerminal(t, persistents.Resource, states.TaskOutcomeSucceeded) == 3
	})
	for _, id := range ids {
		row := h.GetRow(t, id)
		assert.Equal(t, states.TaskPhaseTerminal, row.Phase, "task %d", id)
		assert.Equal(t, states.TaskOutcomeSucceeded, row.Outcome, "task %d", id)
		assert.NotNil(t, row.FinishedAt, "task %d", id)
	}

	// 停机：Run 阻塞到 ctx 取消，返回 ctx.Err()
	cancel()
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(examples.WaitDefault):
		t.Fatal("Run 未随 ctx 取消退出")
	}
}
