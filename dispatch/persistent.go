package dispatch

import (
	"encoding/json"
	"fmt"

	"github.com/improvtrace/stateflux/queue"
)

// 持久化转发协议：带 AttributePersistent 标记的 Signature 不直接投递，由
// Dispatch 包装成 persistent_forward 转发任务、仅发布给 leader 节点；leader
// 侧的 tasks/persistents 收到后先落 task_persistents，再把内层 Signature 解码
// 并重新发布到执行队列。由此任何节点都能提交持久化签名（DB 写入收敛在
// leader），而不必自己落表。
const (
	// AttributePersistent 是持久化标记 header：存在即触发转发（值不参与判断）。
	AttributePersistent = "AttributePersistent"

	// PersistentResource 是转发任务的路由资源名（AttributeResource），分节点
	// 队列为 queue:persistents/<node>。
	PersistentResource = "persistents"

	// PersistentForwardName 是转发任务的 handler 注册名（Signature.Name）。
	PersistentForwardName = "persistent_forward"

	// PersistentForwardRetries 是转发任务的执行内重试次数（Signature.RetryCount）：
	// 落表或重发前的瞬时故障由队列重试兜底。
	PersistentForwardRetries = 3
)

// RoleLeader 是节点表中 leader 节点的角色值（dm 服务约定）。转发任务据此
// 只发布给 leader。
const RoleLeader = "LEADER"

// PersistentForwardPayload 是转发任务的参数（唯一 string Arg 承载的 JSON）：
// Signature 是原 Signature（已去掉 AttributePersistent）的 JSON 编码。
type PersistentForwardPayload struct {
	Signature string `json:"signature"`
}

// forwardSignature 把待持久化的 Signature s1 包装成转发 Signature s2：
// s1 去掉 AttributePersistent 标记后 JSON 编码为 s2 唯一 string Arg 的
// {"signature": ...}，s2 的资源为 persistents、携带 AttributeRoles=LEADER，
// 经 attachRoutingKey 只匹配 leader 节点。s2 不再带标记，重复调用不会二次转发。
//
// Args 的第二参固定 0：转发任务本身无表行（taskID=0），handler 约定签名
// func(payload string, taskID int64) error 要求两个参数。
func forwardSignature(s1 *queue.Signature) (*queue.Signature, error) {
	// 先去标记：s1 会被 leader 重新发布，标记留着会再次触发转发（自循环）
	delete(s1.Headers, AttributePersistent)
	encoded, err := json.Marshal(s1)
	if err != nil {
		return nil, fmt.Errorf("dispatch: encode persistent signature: %w", err)
	}
	payload, err := json.Marshal(PersistentForwardPayload{Signature: string(encoded)})
	if err != nil {
		return nil, fmt.Errorf("dispatch: encode persistent forward payload: %w", err)
	}
	return &queue.Signature{
		Name: PersistentForwardName,
		Args: []queue.Arg{
			{Type: "string", Value: string(payload)},
			{Type: "int64", Value: int64(0)},
		},
		Headers: queue.Headers{
			AttributeResource: PersistentResource,
			AttributeRoles:    RoleLeader,
		},
		RetryCount: PersistentForwardRetries,
	}, nil
}
