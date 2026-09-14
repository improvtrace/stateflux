// Package codec 是任务载荷的 encode/decode 实现（§15.1#13），参考 go machinery 的
// 「签名 + 消息体」设计：
//
//   - machinery 在全局 Signatures 表里注册「任务名 → reflect.Type」，发送时把任务
//     序列化成带签名头的消息，消费端按签名重建具体类型；
//   - 本包等价地把「任务签名 → protobuf 原型构造器」注册在 task.Registry，帧格式为
//     magic + version + uvarint 签名长度 + 签名 + protobuf 载荷。
//
// 与 machinery 的差异：载荷不是 JSON 而是 protobuf 二进制（字段演进与校验更强），
// 但「签名先行、按签名反射重建」的编排完全一致。
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/improvtrace/stateflux/internal/task"
)

// magic 是 codec 帧魔数（'SFXM'）。
var magic = [4]byte{'S', 'F', 'X', 'M'}

const version byte = 1

// ErrBadFrame 表示载荷不是合法 codec 帧。
var ErrBadFrame = errors.New("task/codec: bad frame")

// Codec 基于 task.Registry 做签名化编解码。
type Codec struct {
	registry *task.Registry
}

// New 构造 codec；registry 为 nil 时 panic（装配期错误必须尽早暴露）。
func New(registry *task.Registry) *Codec {
	if registry == nil {
		panic("task/codec: nil registry")
	}
	return &Codec{registry: registry}
}

// Encode 把业务消息编码为带签名头的帧。签名取 protobuf 全名，注册表须能解析它。
func (c *Codec) Encode(msg proto.Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("task/codec: nil message")
	}
	sig := task.Signature(proto.MessageName(msg))
	if sig == "" {
		return nil, errors.New("task/codec: message has no fully-qualified name")
	}
	if !c.registry.Has(sig) {
		return nil, fmt.Errorf("task/codec: unregistered signature %q", sig)
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 5+len(sig)+1+len(body))
	out = append(out, magic[:]...)
	out = append(out, version)
	out = binary.AppendUvarint(out, uint64(len(sig)))
	out = append(out, sig...)
	out = append(out, body...)
	return out, nil
}

// Decode 解析帧并按签名重建消息。
func (c *Codec) Decode(data []byte) (proto.Message, error) {
	sig, body, err := c.split(data)
	if err != nil {
		return nil, err
	}
	msg, err := c.registry.New(sig)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("task/codec: unmarshal %s: %w", sig, err)
	}
	return msg, nil
}

// PeekSignature 只读取签名，不做反序列化（路由/诊断用）。
func (c *Codec) PeekSignature(data []byte) (task.Signature, error) {
	sig, _, err := c.split(data)
	return sig, err
}

func (c *Codec) split(data []byte) (task.Signature, []byte, error) {
	if len(data) < 5 || string(data[:4]) != string(magic[:]) || data[4] != version {
		return "", nil, ErrBadFrame
	}
	rest := data[5:]
	n, m := binary.Uvarint(rest)
	if m <= 0 || uint64(len(rest[m:])) < n {
		return "", nil, ErrBadFrame
	}
	rest = rest[m:]
	sig := task.Signature(rest[:n])
	return sig, rest[n:], nil
}
