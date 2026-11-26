package dispatch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/improvtrace/stateflux/queue"
)

// recordingBroker 记录 Publish 到的 Signature，其余 broker 行为由 BaseBroker
// 兜底；dispatch 的转发路径不需要真实 Redis。
type recordingBroker struct {
	queue.BaseBroker
	published []*queue.Signature
}

func newRecordingBroker(cnf *queue.Config) *recordingBroker {
	return &recordingBroker{BaseBroker: queue.NewBaseBroker(cnf)}
}

func (b *recordingBroker) StartConsuming(string, int, queue.TaskProcessor) (bool, error) {
	return false, nil
}

func (b *recordingBroker) StopConsuming() {}

func (b *recordingBroker) Publish(_ context.Context, sig *queue.Signature) error {
	b.published = append(b.published, sig)
	return nil
}

// stubBackend 只满足 queue.Backend 接口：测试只覆盖 SetStatePending。
type stubBackend struct{}

func (stubBackend) InitGroup(context.Context, string, []string) error { return nil }
func (stubBackend) GroupCompleted(context.Context, string, int) (bool, error) {
	return false, nil
}
func (stubBackend) GroupTaskStates(context.Context, string, int) ([]*queue.TaskState, error) {
	return nil, nil
}
func (stubBackend) TriggerChord(context.Context, string) (bool, error) { return false, nil }
func (stubBackend) SetStatePending(context.Context, *queue.Signature) error {
	return nil
}
func (stubBackend) SetStateReceived(context.Context, *queue.Signature) error { return nil }
func (stubBackend) SetStateStarted(context.Context, *queue.Signature) error  { return nil }
func (stubBackend) SetStateRetry(context.Context, *queue.Signature) error    { return nil }
func (stubBackend) SetStateSuccess(context.Context, *queue.Signature, []*queue.TaskResult) error {
	return nil
}
func (stubBackend) SetStateFailure(context.Context, *queue.Signature, string) error { return nil }
func (stubBackend) GetState(context.Context, string) (*queue.TaskState, error)      { return nil, nil }
func (stubBackend) PurgeState(context.Context, string) error                        { return nil }
func (stubBackend) PurgeGroupMeta(context.Context, string) error                    { return nil }

type stubLock struct{}

func (stubLock) LockWithRetries(context.Context, string, time.Duration) error { return nil }
func (stubLock) Lock(context.Context, string, time.Duration) error            { return nil }

func TestForwardSignature(t *testing.T) {
	s1 := &queue.Signature{
		UUID: "task_42",
		Name: "echo",
		Args: []queue.Arg{
			{Type: "string", Value: `{"asset_id":1}`},
			{Type: "int64", Value: int64(42)},
		},
		Headers: queue.Headers{
			AttributeResource:   "echo",
			AttributePersistent: true,
			AttributeRoles:      "FOLLOWER",
			AttributeLabels:     map[string][]string{"disk": {"ssd"}},
		},
	}

	s2, err := forwardSignature(s1)
	require.NoError(t, err)

	// 内层标记必须去掉：留着会在 leader 重新发布时再次触发转发（自循环）
	_, marked := s1.Headers[AttributePersistent]
	assert.False(t, marked)

	// 转发任务协议：资源 persistents、handler 名 persistent_forward、仅 leader
	assert.Equal(t, PersistentForwardName, s2.Name)
	assert.Equal(t, PersistentResource, s2.Headers[AttributeResource])
	assert.Equal(t, RoleLeader, s2.Headers[AttributeRoles])
	assert.Equal(t, PersistentForwardRetries, s2.RetryCount)
	require.Len(t, s2.Args, 2)
	assert.Equal(t, "int64", s2.Args[1].Type)
	assert.Equal(t, int64(0), s2.Args[1].Value)

	raw, ok := s2.Args[0].Value.(string)
	require.True(t, ok)
	var payload PersistentForwardPayload
	require.NoError(t, json.Unmarshal([]byte(raw), &payload))

	inner := new(queue.Signature)
	decoder := json.NewDecoder(strings.NewReader(payload.Signature))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(inner))
	assert.Equal(t, s1.UUID, inner.UUID)
	assert.Equal(t, s1.Name, inner.Name)
	assert.Equal(t, "echo", inner.Headers[AttributeResource])
	assert.NotContains(t, inner.Headers, AttributePersistent)
}

// TestDispatchForwardsPersistentSignature 断言 Dispatch 对带
// AttributePersistent 的签名不再直投，而是发布 persistent_forward 到 leader
// 节点并返回该次投递的 AsyncResult。
func TestDispatchForwardsPersistentSignature(t *testing.T) {
	broker := newRecordingBroker(&queue.Config{DefaultQueue: "queue:default/node-1"})
	server, err := queue.NewServer(broker.GetConfig(), broker, stubBackend{}, stubLock{})
	require.NoError(t, err)

	d := newTestDispatcher(testNodes...)
	d.server = server

	s1 := &queue.Signature{
		UUID: "task_42",
		Name: "echo",
		Args: []queue.Arg{
			{Type: "string", Value: `{"asset_id":1}`},
			{Type: "int64", Value: int64(42)},
		},
		Headers: queue.Headers{
			AttributeResource:   "echo",
			AttributePersistent: true,
		},
	}
	result, err := d.Dispatch(context.Background(), s1)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, broker.published, 1)
	sent := broker.published[0]
	assert.Equal(t, PersistentForwardName, sent.Name)
	// 仅发布给 leader：testNodes 中只有 node-1 是 LEADER
	assert.Equal(t, QueueName(PersistentResource, "node-1"), sent.RoutingKey)
	assert.Equal(t, result.Signature.UUID, sent.UUID)
}

// TestDispatchWithoutPersistentHeader 断言未标记的签名仍按原逻辑直接投递。
func TestDispatchWithoutPersistentHeader(t *testing.T) {
	broker := newRecordingBroker(&queue.Config{DefaultQueue: "queue:default/node-1"})
	server, err := queue.NewServer(broker.GetConfig(), broker, stubBackend{}, stubLock{})
	require.NoError(t, err)

	d := newTestDispatcher(testNodes...)
	d.server = server

	sig := &queue.Signature{
		UUID:    "task_7",
		Name:    "echo",
		Args:    []queue.Arg{{Type: "string", Value: `{}`}, {Type: "int64", Value: int64(7)}},
		Headers: queue.Headers{AttributeResource: "echo"},
	}
	result, err := d.Dispatch(context.Background(), sig)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, broker.published, 1)
	sent := broker.published[0]
	assert.Equal(t, "task_7", sent.UUID)
	assert.Equal(t, sig.RoutingKey, sent.RoutingKey)
	assert.NotEqual(t, PersistentForwardName, sent.Name)
}
