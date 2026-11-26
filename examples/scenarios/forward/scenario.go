// Package forward 持久化转发场景：生产者只经 Dispatcher 发布带
// AttributePersistent 标记的签名，签名被包装成 persistent_forward 投递给
// leader（dm 桩的 examples-node，也是唯一节点）；leader 侧 tasks/persistents
// 先落 task_persistents 再重新发布内层签名，内层任务由本场景的 echo 任务
// 执行，成功后行收敛为 terminal succeeded。验证 Dispatch 返回的 AsyncResult
// 可查询转发任务状态。
package forward

import (
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
	"github.com/improvtrace/stateflux/tasks"
	"github.com/improvtrace/stateflux/tasks/persistents"
)

// node 是本场景生产方的雪花节点（各场景使用独立节点号，避免 ID 冲突）。
var node, _ = snowflake.NewNode(5)

// Test 生产方经 Dispatcher 提交持久化签名：转发 → 落表 → 重发 → 执行成功。
func Test(h *examples.H, t *testing.T) {
	// 生产方：独立 queue 连接 + Dispatcher（同一 Redis 与 dm 桩，不注册 handler）
	qcnf := &queue.Config{DefaultQueue: dispatch.QueueName("producer", examples.NodeName)}
	rdb := queue.NewGoRedisClient(qcnf, h.RedisAddrs, h.RedisDB)
	defer rdb.Close()
	server, err := queue.NewServer(qcnf,
		queue.NewRedisBrokerWithClient(qcnf, rdb),
		queue.NewRedisBackendWithClient(qcnf, rdb),
		queue.NewRedisLockWithClient(rdb, 1),
	)
	require.NoError(t, err)
	producer := dispatch.New(server, h.DMAddress, examples.DMServiceKey)

	// 执行侧：本地 echo 任务 + 转发消费侧。转发器经同一 Dispatcher 把内层
	// 签名重新发布到 echo 队列，由同一 runtime 的 worker 执行。
	echo := newEchoTask()
	forwarder := persistents.New(h.Repo, producer, examples.NodeName)
	h.StartRuntime(t, []*tasks.TaskSpec{echoSpec(echo), persistents.Spec(forwarder)})

	payload, err := json.Marshal(echoPayload{N: 99})
	require.NoError(t, err)
	id := node.Generate().Int64()
	sig := &queue.Signature{
		UUID: states.SignatureUUID(id),
		Name: echoResource,
		Args: []queue.Arg{
			{Type: "string", Value: string(payload)},
			{Type: "int64", Value: id},
		},
		Headers: queue.Headers{
			dispatch.AttributeResource:   echoResource,
			dispatch.AttributePersistent: true,
		},
	}

	// Dispatch 返回转发任务的 AsyncResult：可查询状态（转发被 leader 处理成功）
	result, err := producer.Dispatch(h.Ctx, sig)
	require.NoError(t, err)
	require.NotNil(t, result)
	_, err = result.GetWithTimeout(h.Ctx, examples.WaitDefault, 100*time.Millisecond)
	require.NoError(t, err)

	// 内层任务由 leader 落表后重发执行，成功回写终态
	examples.WaitFor(t, "转发落表的内层任务执行成功", examples.WaitDefault, func() bool {
		return h.CountTerminal(t, echoResource, states.TaskOutcomeSucceeded) == 1
	})
	row := h.GetRow(t, id)
	assert.Equal(t, states.TaskPhaseTerminal, row.Phase)
	assert.Equal(t, states.TaskOutcomeSucceeded, row.Outcome)
	assert.Equal(t, echoResource, row.Resource)
	assert.JSONEq(t, string(payload), string(row.Payload))
	assert.Len(t, echo.snapshot(), 1, "内层任务应恰好执行一次")
}
