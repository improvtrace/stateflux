package schedule

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
