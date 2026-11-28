package schedule

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

// TestAttributeHeaders 断言表行的目标节点属性注入 Signature headers；值为
// 空（不限）的属性不写 header，交由 dispatch 走下一级规则。
func TestAttributeHeaders(t *testing.T) {
	headers, err := attributeHeaders(&states.TaskPersistent{
		Id: 7, Resource: "echo",
		ClaimedNode: "node-2", Node: "node-3", Label: "disk=ssd;zone=a;zone=b",
		HashBucket: 42, Vpc: "v1",
	})
	assert.NoError(t, err)
	assert.Equal(t, queue.Headers{
		dispatch.AttributeResource:    "echo",
		dispatch.AttributeVpcs:        "v1",
		dispatch.AttributeLabels:      map[string][]string{"disk": {"ssd"}, "zone": {"a", "b"}},
		dispatch.AttributeClaimedNode: "node-2",
		dispatch.AttributeTargetNode:  "node-3",
		dispatch.AttributeHashBucket:  "42",
	}, headers)

	headers, err = attributeHeaders(&states.TaskPersistent{Id: 7, Resource: "echo", Label: " ; "})
	assert.NoError(t, err)
	assert.Equal(t, queue.Headers{dispatch.AttributeResource: "echo"}, headers)

	// label 非法即报错：约束不能被静默放弃
	_, err = attributeHeaders(&states.TaskPersistent{Id: 7, Resource: "echo", Label: "ssd"})
	assert.ErrorContains(t, err, `"ssd"`)
}

// TestSignature 断言从表行组装的 Signature：RoutingKey 留空交 dispatch 路由，
// headers 与 attributeHeaders 一致；label 非法时不组装，任务不发布。
func TestSignature(t *testing.T) {
	reg, err := tasks.NewRegistry(&tasks.TaskSpec{
		Resource: "echo",
		Handler:  func(string, int64) error { return nil },
		Retries:  3,
	})
	assert.NoError(t, err)
	p := NewPublisher(nil, nil, reg, nil, "node-1")

	sig, err := p.signature(&states.TaskPersistent{
		Id: 7, Resource: "echo", Payload: []byte(`{"a":1}`),
		ClaimedNode: "node-1", HashBucket: 9,
	})
	assert.NoError(t, err)
	assert.Equal(t, "task_7", sig.UUID)
	assert.Equal(t, "echo", sig.Name)
	assert.Equal(t, 3, sig.RetryCount)
	assert.Empty(t, sig.RoutingKey)
	assert.Equal(t, queue.Headers{
		dispatch.AttributeResource:    "echo",
		dispatch.AttributeClaimedNode: "node-1",
		dispatch.AttributeHashBucket:  "9",
	}, sig.Headers)

	_, err = p.signature(&states.TaskPersistent{Id: 8, Resource: "echo", Label: "zone"})
	assert.Error(t, err)
}

// claimCapture 捕获 ClaimPending 的认领条件；其余 repo 方法本测试不触达，
// 借接口嵌入保持 nil。
type claimCapture struct {
	states.TaskPersistentRepo
	got *states.TaskPersistentClaim
}

func (c *claimCapture) ClaimPending(_ context.Context, cond *states.TaskPersistentClaim) ([]*states.TaskPersistent, error) {
	c.got = cond
	return []*states.TaskPersistent{}, nil
}

// TestDispatchPendingSkipsDisabled 断言恢复通道只认领启用的资源：resources
// 为空时收敛为启用资源集合（禁用任务的行冻结在 pending）；显式指定资源时
// 原样传递；全部禁用时不触达 repo。
func TestDispatchPendingSkipsDisabled(t *testing.T) {
	reg, err := tasks.NewRegistry(
		&tasks.TaskSpec{Resource: "on", Enabled: true, Handler: func(string, int64) error { return nil }},
		&tasks.TaskSpec{Resource: "off", Handler: func(string, int64) error { return nil }},
	)
	assert.NoError(t, err)
	capture := &claimCapture{}
	p := NewPublisher(capture, nil, reg, nil, "node-1")

	p.DispatchPending(context.Background(), nil)
	require.NotNil(t, capture.got)
	assert.Equal(t, []string{"on"}, capture.got.Resources)

	p.DispatchPending(context.Background(), []string{"off"})
	assert.Equal(t, []string{"off"}, capture.got.Resources, "显式指定的资源原样传递")

	allOff, err := tasks.NewRegistry(&tasks.TaskSpec{Resource: "off2", Handler: func(string, int64) error { return nil }})
	assert.NoError(t, err)
	capture2 := &claimCapture{}
	NewPublisher(capture2, nil, allOff, nil, "node-1").DispatchPending(context.Background(), nil)
	assert.Nil(t, capture2.got, "全部禁用时直接返回，不触达 repo")
}
