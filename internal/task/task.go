// Package task 是任务运行时（§8、§15.1#12）：定义 Task 接口与任务类型注册表，
// 并归组 factory（周期任务生成）与 dispatch（调度侧分发编排）两个子包。
//
// 职责边界：本包只定义「任务是什么」与「如何注册/解析」，不实现具体任务——具体
// factory 由 internal/biz 提供并在装配期注册（§15.1#12）；任务状态的权威仍是 PG
// 四阶段账本，本包不持有任何权威状态。
package task

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Spec 是任务入队规格（对齐 api/stateflux/task/v1.TaskSpec 与 §3.1 任务行公共段）。
// factory 生成任务时填充它，由装配层转换为 PG 写入。
type Spec struct {
	Type           string
	Operator       string
	Priority       int32
	Channel        string
	VPC            string
	Node           string
	Label          string
	HashBucket     int32
	TimeoutMS      int64
	MaxAttempts    int32
	IdempotencyKey string
	// Callback 是 OnSuccess/OnError 回调规格的 jsonb 字节（§5.6，可空）。
	Callback      []byte
	ParentTaskID  int64
	BizRaceLabels []string
	BizRaceEntry  string
	BizGroup      string
	BizBatchID    string
}

// Task 是可生成、可分发、可执行的任务定义（§15.1#12）。
type Task interface {
	// Spec 返回入队规格（不含业务载荷）。
	Spec() Spec
	// Payload 返回业务载荷的序列化字节（§3.3）；nil 表示无载荷。
	Payload() []byte
}

// FuncTask 是 Task 的函数适配器：factory 可用它快速构造内联任务。
type FuncTask struct {
	S Spec
	P []byte
}

// Spec 实现 Task。
func (t FuncTask) Spec() Spec { return t.S }

// Payload 实现 Task。
func (t FuncTask) Payload() []byte { return t.P }

var _ Task = FuncTask{}

// Signature 是任务类型签名：type + operator 共同决定 handler 与编解码原型
// （§3.1「type 粗、operator 细」）。签名是 codec 的注册键（§15.1#13）。
type Signature string

// SignatureOf 由 type 与 operator 生成签名。
func SignatureOf(taskType, operator string) Signature {
	return Signature(taskType + ":" + operator)
}

// Registry 是任务原型注册表（§15.1#12/#13）：对标 machinery 的 Signatures——
// 注册「签名 → 原型消息构造器」，消费端据签名重建具体业务消息。注册表在装配期
// 由 biz 填充，运行期只读。
type Registry struct {
	mu     sync.RWMutex
	protos map[Signature]func() proto.Message
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{
		protos: map[Signature]func() proto.Message{},
	}
}

// Register 注册一个任务原型消息构造函数；签名由消息全名推导。
// 重复注册不同构造函数返回错误（尽早暴露装配冲突）。
func (r *Registry) Register(newFn func() proto.Message) (Signature, error) {
	if newFn == nil {
		return "", errors.New("task: nil prototype constructor")
	}
	msg := newFn()
	if msg == nil {
		return "", errors.New("task: prototype constructor returned nil")
	}
	sig := Signature(proto.MessageName(msg))
	if sig == "" {
		return "", errors.New("task: prototype message has no fully-qualified name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.protos[sig]; ok {
		return "", fmt.Errorf("task: duplicate prototype signature %q", sig)
	}
	r.protos[sig] = newFn
	return sig, nil
}

// New 按签名构造原型消息。
func (r *Registry) New(sig Signature) (proto.Message, error) {
	r.mu.RLock()
	fn, ok := r.protos[sig]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task: unregistered signature %q", sig)
	}
	return fn(), nil
}

// Has 判断签名是否已注册。
func (r *Registry) Has(sig Signature) bool {
	r.mu.RLock()
	_, ok := r.protos[sig]
	r.mu.RUnlock()
	return ok
}

// Signatures 返回全部已注册签名（稳定排序）。
func (r *Registry) Signatures() []Signature {
	r.mu.RLock()
	out := make([]Signature, 0, len(r.protos))
	for sig := range r.protos {
		out = append(out, sig)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
