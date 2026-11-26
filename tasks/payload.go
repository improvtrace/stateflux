package tasks

import (
	"encoding/json"
	"fmt"

	"github.com/improvtrace/stateflux/queue"
)

// 发布约定：复杂业务参数不展开为多个 queue.Arg，统一编码为单个 string 参数
// 承载 JSON（Arg.Type="string"）。handler 签名取一个 string 参数，体内用
// Decode 解回业务类型；调度器组装 Signature.Args 时用 Encode。

// Encode 把业务参数编码为 queue.Arg。
func Encode(v any) (queue.Arg, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return queue.Arg{}, fmt.Errorf("tasks: encode payload: %w", err)
	}
	return queue.Arg{Type: "string", Value: string(b)}, nil
}

// Decode 在 handler 侧把 string 参数解码回业务类型。
func Decode[T any](raw string) (T, error) {
	var v T
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return v, fmt.Errorf("tasks: decode payload: %w", err)
	}
	return v, nil
}
