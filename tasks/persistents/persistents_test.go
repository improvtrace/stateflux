package persistents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
)

// fakeRepo 是 task_persistents 的内存实现，覆盖转发路径用到的
// Create/Claim/MarkDispatched/Requeue 语义。
type fakeRepo struct {
	rows  map[int64]*states.TaskPersistent
	byKey map[string]int64
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		rows:  make(map[int64]*states.TaskPersistent),
		byKey: make(map[string]int64),
	}
}

func (r *fakeRepo) copyRow(row *states.TaskPersistent) *states.TaskPersistent {
	cp := *row
	return &cp
}

func (r *fakeRepo) Create(_ context.Context, m *states.TaskPersistent) (*states.TaskPersistent, error) {
	if m.IdempotencyKey != "" {
		if id, ok := r.byKey[m.IdempotencyKey]; ok {
			return r.copyRow(r.rows[id]), nil
		}
	}
	if _, ok := r.rows[m.Id]; ok {
		return nil, fmt.Errorf("duplicate task id %d", m.Id)
	}
	row := *m
	row.Phase = states.TaskPhasePending
	row.Outcome = states.TaskOutcomeUnset
	row.FinishedAt = nil
	r.rows[row.Id] = &row
	if m.IdempotencyKey != "" {
		r.byKey[m.IdempotencyKey] = m.Id
	}
	return r.copyRow(&row), nil
}

func (r *fakeRepo) Get(_ context.Context, id int64) (*states.TaskPersistent, error) {
	row, ok := r.rows[id]
	if !ok {
		return nil, states.ErrTaskNotFound
	}
	return r.copyRow(row), nil
}

func (r *fakeRepo) ClaimPending(context.Context, *states.TaskPersistentClaim) ([]*states.TaskPersistent, error) {
	// 转发路径不经批量认领
	return nil, nil
}

func (r *fakeRepo) Claim(_ context.Context, id int64, node string) (bool, error) {
	row, ok := r.rows[id]
	if !ok {
		return false, states.ErrTaskNotFound
	}
	if row.Phase != states.TaskPhasePending {
		return false, nil
	}
	row.Phase = states.TaskPhaseDispatching
	row.ClaimedNode = node
	return true, nil
}

func (r *fakeRepo) MarkDispatched(_ context.Context, ids []int64) error {
	for _, id := range ids {
		if row, ok := r.rows[id]; ok && row.Phase == states.TaskPhaseDispatching {
			row.Phase = states.TaskPhaseDispatched
		}
	}
	return nil
}

func (r *fakeRepo) MarkTerminal(_ context.Context, id int64, outcome states.TaskOutcome, summary string) error {
	if row, ok := r.rows[id]; ok &&
		(row.Phase == states.TaskPhaseDispatching || row.Phase == states.TaskPhaseDispatched) {
		row.Phase = states.TaskPhaseTerminal
		row.Outcome = outcome
		row.ErrorSummary = summary
	}
	return nil
}

func (r *fakeRepo) Requeue(_ context.Context, id int64) error {
	if row, ok := r.rows[id]; ok &&
		(row.Phase == states.TaskPhaseDispatching || row.Phase == states.TaskPhaseDispatched) {
		row.Phase = states.TaskPhasePending
	}
	return nil
}

func (r *fakeRepo) Inflight(context.Context, time.Time, int) ([]*states.TaskPersistent, error) {
	return nil, nil
}

// fakePublisher 记录重新发布的 Signature；err 非空时模拟发布失败。
type fakePublisher struct {
	sent []*queue.Signature
	err  error
}

func (p *fakePublisher) Dispatch(_ context.Context, sig *queue.Signature) (*queue.AsyncResult, error) {
	if p.err != nil {
		return nil, p.err
	}
	cp := *sig
	p.sent = append(p.sent, &cp)
	return queue.NewAsyncResult(sig, nil), nil
}

// envelope 组装一条 persistent_forward 转发参数：内层 Signature 的 JSON 放进
// {"signature": ...}。
func envelope(t *testing.T, sig *queue.Signature) string {
	t.Helper()
	encoded, err := json.Marshal(sig)
	require.NoError(t, err)
	payload, err := json.Marshal(dispatch.PersistentForwardPayload{Signature: string(encoded)})
	require.NoError(t, err)
	return string(payload)
}

func innerSignature() *queue.Signature {
	return &queue.Signature{
		UUID: "task_42",
		Name: "echo",
		Args: []queue.Arg{
			{Type: "string", Value: `{"asset_id":1}`},
			{Type: "int64", Value: int64(42)},
		},
		Headers: queue.Headers{
			dispatch.AttributeResource: "echo",
			dispatch.AttributeVpcs:     "v1",
			dispatch.AttributeLabels:   map[string][]string{"disk": {"ssd"}, "zone": {"a", "b"}},
		},
	}
}

// TestHandlePersistsAndRepublishes 断言转发 handler 的核心语义：落表（行号=
// 签名任务 ID）→ 认领 → 重新发布解码后的内层签名 → 确认 dispatched，且表行
// 保留路由属性供对账重派使用。
func TestHandlePersistsAndRepublishes(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	f := New(repo, pub, "leader-1")

	sig := innerSignature()
	require.NoError(t, f.Handle(envelope(t, sig), 0))

	row := repo.rows[42]
	require.NotNil(t, row)
	assert.Equal(t, "echo", row.Resource)
	assert.Equal(t, `{"asset_id":1}`, string(row.Payload))
	assert.Equal(t, "v1", row.Vpc)
	assert.Equal(t, "disk=ssd;zone=a;zone=b", row.Label)
	assert.Equal(t, "task_42", row.IdempotencyKey)
	assert.Equal(t, "leader-1", row.ClaimedNode)
	assert.Equal(t, states.TaskPhaseDispatched, row.Phase)

	require.Len(t, pub.sent, 1)
	sent := pub.sent[0]
	assert.Equal(t, "task_42", sent.UUID)
	assert.Equal(t, "echo", sent.Name)
	assert.Equal(t, int64(42), sent.Args[1].Value)
	// 重发前已剥掉持久化标记，且标签约束还原为 dispatcher 认识的类型
	assert.NotContains(t, sent.Headers, dispatch.AttributePersistent)
	assert.Equal(t, map[string][]string{"disk": {"ssd"}, "zone": {"a", "b"}}, sent.Headers[dispatch.AttributeLabels])
}

// TestHandleIdempotentOnRepeat 断言重复转发（同一签名再次投递）不重复落表、
// 不重复发布。
func TestHandleIdempotentOnRepeat(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	f := New(repo, pub, "leader-1")

	payload := envelope(t, innerSignature())
	require.NoError(t, f.Handle(payload, 0))
	require.NoError(t, f.Handle(payload, 0))

	assert.Len(t, repo.rows, 1)
	assert.Len(t, pub.sent, 1)
}

// TestHandleRequeuesOnPublishFailure 断言重发失败把行退回 pending 并返回错误
// （队列重试后再次转发能成功），已发布成功则不重发。
func TestHandleRequeuesOnPublishFailure(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{err: errors.New("broker down")}
	f := New(repo, pub, "leader-1")

	payload := envelope(t, innerSignature())
	err := f.Handle(payload, 0)
	require.ErrorContains(t, err, "republish")
	assert.Equal(t, states.TaskPhasePending, repo.rows[42].Phase)
	assert.Empty(t, pub.sent)

	// 队列重试：行在 pending，重新认领并发布成功
	pub.err = nil
	require.NoError(t, f.Handle(payload, 0))
	assert.Equal(t, states.TaskPhaseDispatched, repo.rows[42].Phase)
	assert.Len(t, pub.sent, 1)
}

// TestHandleRequiresTaskID 断言生产方必须按发布约定给出任务 ID（第二 Arg 或
// task_{id} UUID），否则拒绝落表。
func TestHandleRequiresTaskID(t *testing.T) {
	f := New(newFakeRepo(), &fakePublisher{}, "leader-1")

	sig := innerSignature()
	sig.UUID = "stateless_echo_1"
	sig.Args[1] = queue.Arg{Type: "int64", Value: int64(0)}
	err := f.Handle(envelope(t, sig), 0)
	require.ErrorContains(t, err, "carries no task id")
}

// TestHandleFallsBackToUUID 断言第二 Arg 缺失任务 ID 时回退解析 UUID 协议。
func TestHandleFallsBackToUUID(t *testing.T) {
	repo := newFakeRepo()
	f := New(repo, &fakePublisher{}, "leader-1")

	sig := innerSignature()
	sig.Args[1] = queue.Arg{Type: "int64", Value: int64(0)}
	require.NoError(t, f.Handle(envelope(t, sig), 0))
	assert.NotNil(t, repo.rows[42])
}

// TestHandleRejectsInvalidPayload 断言非法 payload 不落表。
func TestHandleRejectsInvalidPayload(t *testing.T) {
	repo := newFakeRepo()
	f := New(repo, &fakePublisher{}, "leader-1")

	sig := innerSignature()
	sig.Args[0] = queue.Arg{Type: "string", Value: "not-json"}
	err := f.Handle(envelope(t, sig), 0)
	require.ErrorContains(t, err, "not valid JSON")
	assert.Empty(t, repo.rows)
}
